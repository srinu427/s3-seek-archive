mod compress_utils;
mod header_utils;

pub use compress_utils::CompressionType;
use crossbeam_channel as cc;
use header_utils::S4ArchiveEntryDetails;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::{fs, io, thread, time};
use walkdir::WalkDir;

enum WriteThreadData {
  RawBytes(Vec<u8>),
  TempFile(PathBuf),
  Folder,
}

struct WriteThreadInput {
  data: WriteThreadData,
  compression: CompressionType,
  entry_name: String,
}

fn writer_loop(
  write_buffer_size: u64,
  db_path: PathBuf,
  blob_path: PathBuf,
  rx: cc::Receiver<Option<WriteThreadInput>>,
) -> Result<(), String> {
  let compress_start_time = std::time::Instant::now();

  let conn =
    rusqlite::Connection::open_in_memory().map_err(|e| format!("at in-mem sql db create: {e}"))?;
  let mut header_writer = header_utils::HeaderDBWriter::new(&conn)?;

  // Writer to output file
  let mut buf_writer = io::BufWriter::with_capacity(
    std::cmp::max(128 * 1024, write_buffer_size as usize),
    fs::File::create(&blob_path).map_err(|e| format!("at opening {:?}: {e}", &blob_path))?,
  );

  let mut offset = 0;
  for r_msg in rx {
    let Some(msg) = r_msg else { break };
    match msg.data {
      WriteThreadData::RawBytes(data) => {
        if let Err(e) = buf_writer.write_all(&data) {
          eprintln!("error writing {:?} to output blob: {e}", &msg.entry_name);
          continue;
        }
        let _ = header_writer.insert_entry(S4ArchiveEntryDetails {
          name: msg.entry_name.clone(),
          _type: "FILE".to_string(),
          offset,
          size: data.len() as u64,
          compression: msg.compression
        })
          .inspect_err(|e| eprintln!("{e}"));
        offset += data.len() as u64;
      }
      WriteThreadData::TempFile(temp_file) => {
        let Ok(fr) = fs::File::open(&temp_file).inspect_err(|e| {
          eprintln!(
            "can't open temp file {:?}: {e}. externally modified?. skipping it",
            &temp_file
          )
        }) else {
          continue;
        };
        let mut buf_reader = io::BufReader::with_capacity(128 * 1024, fr);
        let write_size = io::copy(&mut buf_reader, &mut buf_writer)
          .inspect_err(|e| eprintln!("error writing to blob file: {e}"))
          .unwrap_or(0);
        let _ =
          fs::remove_file(&temp_file).inspect_err(|e| eprintln!("error removing temp file: {e}"));
        let _ = header_writer.insert_entry(S4ArchiveEntryDetails {
            name: msg.entry_name.clone(),
            _type: "FILE".to_string(),
            offset,
            size: write_size,
            compression: msg.compression
          })
            .inspect_err(|e| eprintln!("{e}"));
        offset += write_size;
      }
      WriteThreadData::Folder => {
        let _ = header_writer.insert_entry(S4ArchiveEntryDetails {
          name: msg.entry_name.clone(),
          _type: "FOLDER".to_string(),
          offset: 0u64,
          size: 0u64,
          compression: msg.compression
        })
          .inspect_err(|e| eprintln!("{e}"));
      }
    }
  }

  header_writer.flush()?;
  buf_writer.flush().map_err(|e| format!("error flushing data to blob: {e}"))?;

  // Dump sqlite db to file
  let mut disk_db_conn =
    rusqlite::Connection::open(&db_path).map_err(|e| format!("at creating temp db file: {e}"))?;
  let db_backup_handle = rusqlite::backup::Backup::new(&conn, &mut disk_db_conn)
    .map_err(|e| format!("at flushing data to index: {e}"))?;
  db_backup_handle
    .run_to_completion(5, time::Duration::from_nanos(0), None)
    .map_err(|e| format!("at flushing data to index: {e}"))?;
  let compress_end_time = std::time::Instant::now();
  println!("Compression time: {:.2}s", (compress_end_time - compress_start_time).as_secs_f32());
  Ok(())
}

pub fn mux_db_and_blob(db_path: &Path, blob_path: &Path) -> Result<(), String> {
    let muxing_start_time = std::time::Instant::now();
    println!("muxing {db_path:?} and {blob_path:?}");
    // compress sqlite db file
    let output_db_lz = PathBuf::from(format!("{}.xz", db_path.to_string_lossy()));
    compress_utils::compress(&db_path, &output_db_lz, CompressionType::LZMA)
      .map_err(|e| format!("at compressing db file: {e}"))?;
    let db_path_str = db_path.to_string_lossy().to_string();
    let output_name = PathBuf::from(db_path_str.strip_suffix(".db").unwrap_or(&db_path_str));
    let mut s4a_writer =
      fs::File::create(&output_name).map_err(|e| format!("at opening {:?}: {e}", &output_name))?;
    let output_db_size =
      output_db_lz.metadata().map_err(|e| format!("at getting db file size: {e}"))?.len();
    s4a_writer
      .write(&output_db_size.to_be_bytes())
      .map_err(|e| format!("at writing db size to s4a file: {e}"))?;
    let mut output_db_fr =
      fs::File::open(&output_db_lz).map_err(|e| format!("at opening {:?}: {e}", &output_db_lz))?;
    io::copy(&mut output_db_fr, &mut s4a_writer)
      .map_err(|e| format!("at writing to s4a file: {e}"))?;
    let mut output_blob_fr =
      fs::File::open(blob_path).map_err(|e| format!("at opening {blob_path:?}: {e}"))?;
    io::copy(&mut output_blob_fr, &mut s4a_writer)
      .map_err(|e| format!("at writing blob to s4a file: {e}"))?;
    s4a_writer.flush().map_err(|e| format!("at flushing data after muxing: {e}"))?;

    let _ = fs::remove_file(&output_db_lz)
      .inspect_err(|e| eprintln!("at deleting temp file {:?}: {e}", &output_db_lz));
    let _ = fs::remove_file(db_path)
      .inspect_err(|e| eprintln!("at deleting temp file {db_path:?}: {e}"));
    let _ = fs::remove_file(blob_path)
      .inspect_err(|e| eprintln!("at deleting temp file {blob_path:?}: {e}"));

    let muxing_end_time = std::time::Instant::now();
    println!("Muxing time: {:.2}s", (muxing_end_time - muxing_start_time).as_secs_f32());
    Ok(())
}

pub fn compress_directory(
  dir_path: &Path,
  output_path: &Path,
  num_threads: u32,
  compression: CompressionType,
  mux: bool,
  max_in_mem_file_size: u64,
  write_buffer_size: u64,
) -> Result<(), String> {
  let list_file_start_time = std::time::Instant::now();
  println!("Getting list of entries to archive");
  let entry_list = WalkDir::new(&dir_path)
    .into_iter()
    .filter_map(|r_dir_entry| {
      r_dir_entry.inspect_err(|e| eprintln!("error reading entry: {e}. skipping it")).ok()
    })
    .collect::<Vec<_>>();
  let list_file_end_time = std::time::Instant::now();
  println!("List file time: {:.2}s", (list_file_end_time - list_file_start_time).as_secs_f32());
  println!("{} entries to be archived", entry_list.len());

  let tmp_dir =
    tempfile::tempdir().map_err(|e| format!("at creating temp dir for compression: {e}"))?;
  let tmp_dir_path = tmp_dir.path();

  // Thread pool
  let t_pool = rayon::ThreadPoolBuilder::new()
    .num_threads(num_threads as usize)
    .build()
    .map_err(|e| format!("error initializing thread pool: {e}"))?;

  let (tx, rx) = cc::bounded(std::cmp::max(num_threads as _, 256));
  let output_path_owned = output_path.to_path_buf();
  let t_handle = thread::spawn(move || {
    let blob_path = PathBuf::from(format!("{}.blob", output_path_owned.to_string_lossy()));
    let db_path = PathBuf::from(format!("{}.db", output_path_owned.to_string_lossy()));
    let _ = writer_loop(write_buffer_size, db_path, blob_path, rx)
      .inspect_err(|e| eprintln!("writer thread error: {e}"));
  });

  t_pool.scope(|s| {
    for entry in entry_list {
      if t_handle.is_finished() {
        eprintln!("writer thread stopped unexpectedly. stopping");
        return;
      }
      let entry_name =
        entry.path().strip_prefix(dir_path).unwrap_or(entry.path()).to_string_lossy().to_string();
      if entry_name == "" {
        continue;
      }
      if entry.path().is_file() {
        let temp_file_path =
          tmp_dir_path.join(format!("{}.xz", entry_name.replace("\\", "#").replace("/", "#")));
        let tx_thread_owned = tx.clone();
        s.spawn(move |_| {
          let file_len = fs::metadata(entry.path()).map(|x| x.len()).unwrap_or(u64::MAX);
          if file_len > max_in_mem_file_size {
            let _ = compress_utils::compress(entry.path(), &temp_file_path, compression)
              .inspect_err(|e| eprintln!("error compressing {:?}: {e}", entry.path()))
              .map(|_| {
                let _ = tx_thread_owned
                  .send(Some(WriteThreadInput {
                    data: WriteThreadData::TempFile(temp_file_path),
                    compression,
                    entry_name,
                  }))
                  .inspect_err(|e| {
                    eprintln!("error writing {:?} to archive: {e}", entry.path());
                  });
              });
          } else {
            let _ = compress_utils::compress_in_mem(entry.path(), compression)
              .inspect_err(|e| eprintln!("error compressing {:?}: {e}", entry.path()))
              .map(|data| {
                let _ = tx_thread_owned
                  .send(Some(WriteThreadInput {
                    data: WriteThreadData::RawBytes(data),
                    compression,
                    entry_name,
                  }))
                  .inspect_err(|e| {
                    eprintln!("error writing {:?} to archive: {e}", entry.path());
                  });
              });
          }
        });
      } else if entry.path().is_dir() {
        let _ = tx
          .send(Some(WriteThreadInput {
            data: WriteThreadData::Folder,
            compression,
            entry_name,
          }))
          .inspect_err(|e| eprintln!("error writing {:?} to archive: {e}", entry.path()));
      }
    }
  });

  let _ = tx.send(None).inspect_err(|e| eprintln!("error stopping writer: {e}"));
  let _ = t_handle.join();

  if mux {
    let blob_path = PathBuf::from(format!("{}.blob", output_path.to_string_lossy()));
    let db_path = PathBuf::from(format!("{}.db", output_path.to_string_lossy()));
    mux_db_and_blob(&db_path,& blob_path)?;
  }

  fs::remove_dir_all(&tmp_dir_path).map_err(|e| format!("error removing temp dir: {e}"))?;
  Ok(())
}

pub fn extract_db(inp: &Path, out: &Path) -> Result<u64, String> {
  if !inp.to_string_lossy().ends_with(".s4a") {
    return Err("expecting .s4a file to extract header from".to_string());
  }
  let mut fr = fs::File::open(inp)
    .map_err(|e| format!("at opening archive {inp:?}: {e}"))?;
  let mut header_size_bytes = [0u8; 8];
  fr.read_exact(&mut header_size_bytes).map_err(|e| format!("at reading header size: {e}"))?;
  let header_size = u64::from_be_bytes(header_size_bytes);
  let mut header_bytes = vec![0u8; header_size as usize];
  fr.read_exact_at(&mut header_bytes, 8).map_err(|e| format!("at reading header bytes: {e}"))?;
  let header_bytes_uncompressed = compress_utils::decompress_from_mem(&header_bytes, CompressionType::LZMA)
    .map_err(|e| format!("at extracting header bytes: {e}"))?;
  let mut fw = fs::File::create(out)
    .map_err(|e| format!("at creating header file {out:?}: {e}"))?;
  fw.write(&header_bytes_uncompressed)
    .map_err(|e| format!("at writing header to file: {e}"))?;
  Ok(header_size + 8)
}

pub fn demux_s4a(inp: &Path) -> Result<(), String> {
  let db_path = PathBuf::from(format!("{}.db", inp.to_string_lossy()));
  let blob_path = PathBuf::from(format!("{}.blob", inp.to_string_lossy()));
  let blob_offset = extract_db(inp, &db_path)?;

  let mut fr = fs::File::open(inp).map_err(|e| format!("at opening {inp:?}: {e}"))?;
  let mut fw = fs::File::create(&blob_path).map_err(|e| format!("at opening {:?}: {e}", &blob_path))?;
  fr.seek(SeekFrom::Start(blob_offset)).map_err(|e| format!("at seeking to blob: {e}"))?;
  io::copy(&mut fr, &mut fw).map_err(|e| format!("at copying blob: {e}"))?;
  fw.flush().map_err(|e| format!("at flushing blob: {e}"))?;
  Ok(())
}

pub struct LocalS4ArchiveReader {
  entry_map: HashMap<String, S4ArchiveEntryDetails>,
  archive_path: PathBuf,
  blob_offset: u64,
}

impl LocalS4ArchiveReader {
  pub fn from_s4a(archive_path: &Path) -> Result<Self, String> {
    let temp_dir = tempfile::tempdir().map_err(|e| format!("at tmp dir create: {e}"))?;
    let temp_header_file = temp_dir.path().join("header_db.db");
    let blob_offset = extract_db(archive_path, &temp_header_file)?;
    let entry_map = S4ArchiveEntryDetails::parse_header(&temp_header_file)?;
    Ok(Self { entry_map, archive_path: archive_path.to_path_buf(), blob_offset })
  }

  pub fn from_s4a_db(db_path: &Path) -> Result<Self, String> {
    let entry_map = S4ArchiveEntryDetails::parse_header(db_path)?;
    let blob_path = db_path.with_extension("blob");
    Ok(Self { entry_map, archive_path: blob_path, blob_offset: 0 })
  }

  fn extract_entry(
    &self,
    entry_info: S4ArchiveEntryDetails,
    output_dir: PathBuf,
  ) -> Result<(), String> {
    let out_file_name = output_dir.join(&entry_info.name);
    if entry_info._type == "FILE" {
      out_file_name.parent().map(fs::create_dir_all);
      let fr = fs::File::open(&self.archive_path).map_err(|e| format!("at opening blob: {e}"))?;
      let mut reader = io::BufReader::with_capacity(128 * 1024, fr);
      let mut fw = fs::File::create(&out_file_name)
        .map_err(|e| format!("at opening {:?}: {e}", &out_file_name))?;
      reader
        .seek(SeekFrom::Start(self.blob_offset + entry_info.offset))
        .map_err(|e| format!("at seeking in blob file: {e}"))?;
      let chunk_reader = reader.take(entry_info.size);
      compress_utils::decompress_stream(chunk_reader, &mut fw, entry_info.compression)
        .map_err(|e| format!("at decompressing {:?}: {e}", &output_dir))?;
      Ok(())
    } else if entry_info._type == "FOLDER" {
      let _ = fs::create_dir_all(&out_file_name)
        .map_err(|e| format!("at create dir {:?}: {e}", &out_file_name))?;
      Ok(())
    } else {
      Err(format!("invalid entry type \"{}\" for {}", &entry_info._type, &entry_info.name))
    }
  }

  pub fn extract_files(&self, file_names: &[&str], output_dir: &Path) {
    for f_name in file_names {
      let Some(entry_info) = self.entry_map.get(*f_name) else {
        eprintln!("can't find {} in archive. skipping", *f_name);
        continue;
      };
      let out_file_name = output_dir.join(&entry_info.name);
      let _ = self
        .extract_entry(entry_info.clone(), out_file_name)
        .inspect_err(|e| eprintln!("error while extracting: {e}. skipping"));
    }
  }

  pub fn extract_all_files(&self, output_dir: &Path) {
    self.entry_map.clone().into_par_iter().for_each(|(_, entry_info)| {
      if let Err(e) = self.extract_entry(entry_info, output_dir.to_path_buf()) {
        eprintln!("error while extracting: {e}. skipping")
      }
    });
  }

  pub fn extract_regexp_files(&self, output_dir: &Path, pattern: &str) {
    let re_obj = regex::Regex::new(pattern)
      .inspect_err(|e| eprintln!("invalid regex \"{pattern}\": {e}"));
    let Ok(re_obj) = re_obj else { return; };
    self.entry_map.clone().into_par_iter().for_each(|(_, entry_info)| {
      if !re_obj.is_match(&entry_info.name) {
        return;
      }
      if let Err(e) = self.extract_entry(entry_info, output_dir.to_path_buf()) {
        eprintln!("error while extracting: {e}. skipping")
      }
    });
  }
}

pub fn uncompress_archive(
  archive_path: &Path,
  output_path: &Path,
  _num_threads: u32,
  pattern: &str,
) -> Result<(), String> {
  let archive_reader = if archive_path.to_string_lossy().ends_with(".s4a") {
    LocalS4ArchiveReader::from_s4a(archive_path)?
  } else if archive_path.to_string_lossy().ends_with(".s4a.db") {
    LocalS4ArchiveReader::from_s4a_db(archive_path)?
  } else {
    return Err("unknown file extension. Expecting a .s4a ot .s4a.db".to_string());
  };
  archive_reader.extract_regexp_files(output_path, pattern);
  Ok(())
}
