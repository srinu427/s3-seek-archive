mod compress_utils;

pub use compress_utils::CompressionType;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc;
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
  output_name: PathBuf,
  rx: mpsc::Receiver<Option<WriteThreadInput>>,
) -> Result<(), String> {
  let output_blob = PathBuf::from(format!("{}.blob", output_name.to_string_lossy()));

  let conn =
    rusqlite::Connection::open_in_memory().map_err(|e| format!("at in-mem sql db create: {e}"))?;
  // Create table
  conn
    .execute(
      "CREATE TABLE entry_list (
        name VARCHAR(2048),
        type VARCHAR(8),
        offset BIGINT,
        size BIGINT,
        compression VARCHAR(8)
      )",
      [],
    )
    .map_err(|e| format!("at creating mysql table: {e}"))?;

  // Prepare SQL insert statement
  let mut insert_row_stmt = conn
    .prepare("INSERT INTO entry_list 
      (name, type, offset, size, compression) VALUES (?1, ?2, ?3, ?4, ?5)")
    .map_err(|e| format!("error preparing insert statement: {e}"))?;

  // Writer to output file
  let mut buf_writer = io::BufWriter::with_capacity(
    max(128 * 1024, write_buffer_size as usize),
    fs::File::create(&output_blob).map_err(|e| format!("at opening {:?}: {e}", &output_name))?,
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
        let _ = insert_row_stmt
          .execute((&msg.entry_name, "FILE", offset, data.len() as u64, msg.compression.to_string()))
          .inspect_err(|e| eprintln!("error adding {} to index: {e}", &msg.entry_name));
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
        let _ = insert_row_stmt
          .execute((&msg.entry_name, "FILE", offset, write_size, msg.compression.to_string()))
          .inspect_err(|e| eprintln!("error adding {} to index: {e}", &msg.entry_name));
        offset += write_size;
      }
      WriteThreadData::Folder => {
        let _ = insert_row_stmt
          .execute((&msg.entry_name, "FOLDER", 0u64, 0u64, msg.compression.to_string()))
          .inspect_err(|e| eprintln!("error adding {} to index: {e}", &msg.entry_name));
      }
    }
  }

  let _ = insert_row_stmt.finalize().map_err(|e| eprintln!("error flushing data to index: {e}"));
  buf_writer.flush().map_err(|e| format!("error flushing data to blob: {e}"))?;

  // Dump sqlite db to file
  let output_db = PathBuf::from(format!("{}.db", output_name.to_string_lossy()));
  let mut disk_db_conn =
    rusqlite::Connection::open(&output_db).map_err(|e| format!("at creating temp db file: {e}"))?;
  let db_backup_handle = rusqlite::backup::Backup::new(&conn, &mut disk_db_conn)
    .map_err(|e| format!("at flushing data to index: {e}"))?;
  db_backup_handle
    .run_to_completion(5, time::Duration::from_nanos(0), None)
    .map_err(|e| format!("at flushing data to index: {e}"))?;

  // compress sqlite db file
  let output_db_lz = PathBuf::from(format!("{}.db.xz", output_name.to_string_lossy()));
  let output_db_size = compress_utils::compress(&output_db, &output_db_lz, CompressionType::LZMA)
    .map_err(|e| format!("at compressing db file: {e}"))?;

  println!("muxing db and blob");
  let s4a_writer =
    fs::File::create(&output_name).map_err(|e| format!("at opening {:?}: {e}", &output_name))?;
  let mut s4a_writer = io::BufWriter::with_capacity(write_buffer_size as usize, s4a_writer);
  // let output_db_size =
  //   output_db_lz.metadata().map_err(|e| format!("at getting db file size: {e}"))?.len();
  s4a_writer
    .write(&output_db_size.to_be_bytes())
    .map_err(|e| format!("at writing db size to s4a file: {e}"))?;
  let mut output_db_fr =
    fs::File::open(&output_db_lz).map_err(|e| format!("at opening {:?}: {e}", &output_db_lz))?;
  io::copy(&mut output_db_fr, &mut s4a_writer)
    .map_err(|e| format!("at writing to s4a file: {e}"))?;
  let output_blob_fr =
    fs::File::open(&output_blob).map_err(|e| format!("at opening {:?}: {e}", &output_blob))?;
  let mut output_blob_fr = io::BufReader::with_capacity(write_buffer_size as usize, output_blob_fr);
  io::copy(&mut output_blob_fr, &mut s4a_writer)
    .map_err(|e| format!("at writing blob to s4a file: {e}"))?;
  s4a_writer.flush().map_err(|e| format!("at flushing data after muxing: {e}"))?;

  let _ = fs::remove_file(&output_db_lz)
    .inspect_err(|e| eprintln!("at deleting temp file {:?}: {e}", &output_db_lz));
  let _ = fs::remove_file(&output_db)
    .inspect_err(|e| eprintln!("at deleting temp file {:?}: {e}", &output_db));
  let _ = fs::remove_file(&output_blob)
    .inspect_err(|e| eprintln!("at deleting temp file {:?}: {e}", &output_blob));
  Ok(())
}

pub fn compress_directory(
  dir_path: &Path,
  output_path: &Path,
  num_threads: u32,
  compression: CompressionType,
  max_in_mem_file_size: u64,
  write_buffer_size: u64,
) -> Result<(), String> {
  println!("Getting list of entries to archive");
  let entry_list = WalkDir::new(&dir_path)
    .into_iter()
    .filter_map(|r_dir_entry| {
      r_dir_entry.inspect_err(|e| eprintln!("error reading entry: {e}. skipping it")).ok()
    })
    .collect::<Vec<_>>();
  println!("{} entries to be archived", entry_list.len());

  let tmp_dir =
    tempfile::tempdir().map_err(|e| format!("at creating temp dir for compression: {e}"))?;
  let tmp_dir_path = tmp_dir.path();

  // Thread pool
  let t_pool = rayon::ThreadPoolBuilder::new()
    .num_threads(num_threads as usize)
    .build()
    .map_err(|e| format!("error initializing thread pool: {e}"))?;

  let (tx, rx) = mpsc::channel();
  let output_path_owned = output_path.to_path_buf();
  let t_handle = thread::spawn(move || {
    let _ = writer_loop(write_buffer_size, output_path_owned, rx)
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

  fs::remove_dir_all(&tmp_dir_path).map_err(|e| format!("error removing temp dir: {e}"))?;
  Ok(())
}

#[derive(Clone)]
pub struct S4ArchiveEntryDetails {
  name: String,
  _type: String,
  offset: u64,
  size: u64,
  compression: CompressionType,
}

impl S4ArchiveEntryDetails {
  pub fn parse_header<P: AsRef<Path> + Debug + ?Sized>(
    header_file: &P,
  ) -> Result<HashMap<String, Self>, String> {
    let conn = rusqlite::Connection::open(header_file)
      .map_err(|e| format!("error opening db file {header_file:?}: {e}"))?;
    let mut entry_query = conn
      .prepare("SELECT name, type, offset, size, compression FROM entry_list")
      .map_err(|e| format!("sql query mistake: {e}"))?;
    let entry_map = entry_query
      .query_map([], |row| {
        Ok(S4ArchiveEntryDetails {
          name: row.get(0)?,
          _type: row.get(1)?,
          offset: row.get(2)?,
          size: row.get(3)?,
          compression: CompressionType::from_str(&row.get::<usize, String>(4)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        })
      })
      .map_err(|e| format!("iterating through header failed: {e}"))?
      .filter_map(|x| x.inspect_err(|e| eprintln!("error parsing db entry: {e}, skipping")).ok())
      .map(|s4a_entry| (s4a_entry.name.clone(), s4a_entry))
      .collect::<HashMap<_, _>>();
    Ok(entry_map)
  }
}

pub struct LocalS4ArchiveReader {
  entry_map: HashMap<String, S4ArchiveEntryDetails>,
  archive_path: PathBuf,
  blob_offset: u64,
}

impl LocalS4ArchiveReader {
  pub fn from(archive_path: &Path) -> Result<Self, String> {
    let mut fr = fs::File::open(archive_path)
      .map_err(|e| format!("at opening archive {:?}: {e}", archive_path))?;
    let mut header_size_bytes = [0u8; 8];
    fr.read_exact(&mut header_size_bytes).map_err(|e| format!("at reading header size: {e}"))?;
    let header_size = u64::from_be_bytes(header_size_bytes);
    let mut header_bytes = vec![0u8; header_size as usize];
    fr.read_exact_at(&mut header_bytes, 8).map_err(|e| format!("at reading header bytes: {e}"))?;
    let header_bytes_uncompressed = compress_utils::decompress_from_mem(&header_bytes, CompressionType::LZMA)
      .map_err(|e| format!("at extracting header bytes: {e}"))?;
    let temp_dir = tempfile::tempdir().map_err(|e| format!("at tmp dir create: {e}"))?;
    let temp_header_file = temp_dir.path().join("header_db.db");
    let mut fw = fs::File::create(&temp_header_file)
      .map_err(|e| format!("at creating tmp header file: {e}"))?;
    fw.write(&header_bytes_uncompressed)
      .map_err(|e| format!("at writing header bytes to temp file: {e}"))?;
    let entry_map = S4ArchiveEntryDetails::parse_header(&temp_header_file)?;
    Ok(Self { entry_map, archive_path: archive_path.to_path_buf(), blob_offset: header_size + 8 })
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
  let archive_reader = LocalS4ArchiveReader::from(archive_path)?;
  archive_reader.extract_regexp_files(output_path, pattern);
  Ok(())
}
