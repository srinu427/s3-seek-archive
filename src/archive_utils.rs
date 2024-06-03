mod compress_utils;

use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::{fs, io, thread};
use walkdir::WalkDir;

struct WriteThreadInput {
  temp_file: Option<PathBuf>,
  entry_name: String,
}

fn writer_loop(
  output_name: PathBuf,
  rx: mpsc::Receiver<Option<WriteThreadInput>>,
) -> Result<(), String> {
  let output_blob = PathBuf::from(format!("{}.s4a.blob", output_name.to_string_lossy()));
  let output_db = PathBuf::from(format!("{}.s4a.db", output_name.to_string_lossy()));

  if output_db.exists() {
    if output_db.is_file() {
      println!("{:?} already exists replacing it", &output_db);
      fs::remove_file(&output_db).map_err(|e| format!("error deleting {:?}: {e}", &output_db))?;
    } else {
      return Err(format!("{:?} already exists and is not a file", &output_db));
    }
  }

  let conn = rusqlite::Connection::open(&output_db)
    .map_err(|e| format!("error creating temp db file: {e}"))?;
  // Create table
  conn
    .execute(
      "CREATE TABLE entry_list \
    (name VARCHAR(2048), type VARCHAR(8), offset BIGINT, size BIGINT)",
      [],
    )
    .map_err(|e| format!("error creating mysql table: {e}"))?;

  // Prepare SQL insert statement
  let mut insert_row_stmt = conn
    .prepare("INSERT INTO entry_list (name, type, offset, size) VALUES (?1, ?2, ?3, ?4)")
    .map_err(|e| format!("error preparing insert statement: {e}"))?;

  // Writer to output file
  let mut buf_writer = io::BufWriter::with_capacity(
    128 * 1024,
    fs::File::create(&output_blob)
      .map_err(|e| format!("error opening file {:?}: {e}", &output_name))?,
  );

  let mut offset = 0;
  for r_msg in rx {
    if let Some(msg) = r_msg {
      if let Some(temp_file) = &msg.temp_file {
        match fs::File::open(temp_file) {
          Ok(fr) => {
            let mut buf_reader = io::BufReader::with_capacity(128 * 1024, fr);
            let write_size = io::copy(&mut buf_reader, &mut buf_writer).unwrap_or(0);
            let _ = insert_row_stmt
              .execute((&msg.entry_name, "FILE", offset, write_size))
              .inspect_err(|e| eprintln!("error adding {} to index: {e}", &msg.entry_name));
            offset += write_size;
          }
          Err(e) => eprintln!(
            "can't open temp file {:?}: {e}. externally modified?. skipping it",
            temp_file
          ),
        }
        let _ =
          fs::remove_file(temp_file).inspect_err(|e| eprintln!("error removing temp file: {e}"));
      } else {
        let _ = insert_row_stmt
          .execute((&msg.entry_name, "FOLDER", 0u64, 0u64))
          .inspect_err(|e| println!("error adding {} to index: {e}", &msg.entry_name));
      }
    } else {
      break;
    }
  }
  let _ = insert_row_stmt.finalize().map_err(|e| eprintln!("error flushing data to index: {e}"));

  Ok(())
}

pub fn compress_directory(
  dir_path: &Path,
  output_path: &Path,
  num_threads: u32,
) -> Result<(), String> {
  println!("Getting list of entries to archive");
  let entry_list = WalkDir::new(&dir_path)
    .into_iter()
    .filter_map(|r_dir_entry| {
      r_dir_entry.inspect_err(|e| eprintln!("error reading entry: {e}. skipping it")).ok()
    })
    .collect::<Vec<_>>();
  println!("{} entries to be archived", entry_list.len());

  let tmp_dir_path =
    PathBuf::from(format!("{}_tmp_{}", output_path.with_file_name("s4a_temp").display(), 0));
  fs::create_dir_all(&tmp_dir_path).map_err(|e| format!("error creating temp dir: {e}"))?;

  // Thread pool
  let t_pool = rayon::ThreadPoolBuilder::new()
    .num_threads(num_threads as usize)
    .build()
    .map_err(|e| format!("error initializing thread pool: {e}"))?;

  let (tx, rx) = mpsc::channel();
  let output_path_owned = output_path.to_path_buf();
  let t_handle = thread::spawn(|| {
    let _ =
      writer_loop(output_path_owned, rx).inspect_err(|e| eprintln!("writer thread error: {e}"));
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
          let _ = compress_utils::compress_lzma(entry.path(), &temp_file_path)
            .inspect_err(|e| eprintln!("error compressing {:?}: {e}", entry.path()))
            .map(|_| {
              let _ = tx_thread_owned
                .send(Some(WriteThreadInput { temp_file: Some(temp_file_path), entry_name }))
                .inspect_err(|e| {
                  eprintln!("error writing {:?} to archive: {e}", entry.path());
                });
            });
        });
      } else if entry.path().is_dir() {
        let _ = tx
          .send(Some(WriteThreadInput { temp_file: None, entry_name }))
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
struct S4ArchiveEntryDetails {
  name: String,
  _type: String,
  offset: u64,
  size: u64,
}

impl S4ArchiveEntryDetails {
  pub fn parse_header<P: AsRef<Path> + Debug + ?Sized>(
    header_file: &P,
  ) -> Result<HashMap<String, Self>, String> {
    let conn = rusqlite::Connection::open(header_file)
      .map_err(|e| format!("error opening db file {header_file:?}: {e}"))?;
    let mut entry_query = conn
      .prepare("SELECT name, type, offset, size FROM entry_list")
      .map_err(|e| format!("sql query mistake: {e}"))?;
    let entry_map = entry_query
      .query_map([], |row| {
        Ok(S4ArchiveEntryDetails {
          name: row.get(0)?,
          _type: row.get(1)?,
          offset: row.get(2)?,
          size: row.get(3)?,
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
  blob_path: PathBuf,
}

impl LocalS4ArchiveReader {
  pub fn from(header: &Path, blob: &Path) -> Result<Self, String> {
    let entry_map = S4ArchiveEntryDetails::parse_header(header)?;
    Ok(Self { entry_map, blob_path: blob.to_path_buf() })
  }

  fn extract_entry(
    &self,
    entry_info: S4ArchiveEntryDetails,
    output_dir: PathBuf,
  ) -> Result<(), String> {
    let out_file_name = output_dir.join(&entry_info.name);
    if entry_info._type == "FILE" {
      out_file_name.parent().map(fs::create_dir_all);
      let fr = fs::File::open(&self.blob_path).map_err(|e| format!("at opening blob: {e}"))?;
      let mut reader = io::BufReader::with_capacity(128 * 1024, fr);
      let fw = fs::File::create(&out_file_name)
        .map_err(|e| format!("at opening {:?}: {e}", &out_file_name))?;
      let mut writer =
        lzma::LzmaWriter::new_decompressor(fw).map_err(|e| format!("at decompressor init: {e}"))?;
      reader
        .seek(SeekFrom::Start(entry_info.offset))
        .map_err(|e| format!("at seeking in blob file: {e}"))?;
      let mut chunk_reader = reader.take(entry_info.size);
      io::copy(&mut chunk_reader, &mut writer).map_err(|e| format!("at decompressing: {e}"))?;
      Ok(())
    } else if entry_info._type == "FOLDER" {
      let _ = fs::create_dir_all(&out_file_name)
        .map_err(|e| format!("at create dir {:?}: {e}", &out_file_name))?;
      Ok(())
    } else {
      Err(format!("invalid entry type \"{}\" for {}", &entry_info._type, &entry_info.name))
    }
  }

  pub fn extract_files(&self, file_names: &[&str], output_dir: &Path) -> Result<(), String> {
    match fs::File::open(&self.blob_path) {
      Ok(fr) => {
        let mut o_reader = Some(io::BufReader::with_capacity(128 * 1024, fr));
        for f_name in file_names {
          let Some(entry_info) = self.entry_map.get(*f_name) else {
            eprintln!("can't find {} in archive. skipping", *f_name);
            continue;
          };
          let out_file_name = output_dir.join(&entry_info.name);
          if entry_info._type == "FOLDER" {
            let _ = fs::create_dir_all(&out_file_name)
              .inspect_err(|e| eprintln!("cant create dir {:?}: {e}. skipping", &out_file_name));
            continue;
          } else if entry_info._type == "FILE" {
            out_file_name.parent().map(fs::create_dir_all);
          } else {
            eprintln!(
              "invalid entry type \"{}\" for {}. skipping",
              &entry_info._type, &entry_info.name
            );
            continue;
          }

          match fs::File::create(&out_file_name) {
            Ok(fw) => {
              let Ok(mut writer) = lzma::LzmaWriter::new_decompressor(fw)
                .inspect_err(|e| eprintln!("error initializing decompressor: {e}"))
              else {
                continue;
              };
              let Some(mut reader) = o_reader.take() else { continue };
              if let Err(e) = reader.seek(SeekFrom::Start(entry_info.offset)) {
                o_reader = Some(reader);
                eprintln!("error seeking in blob file: {e}");
                continue;
              }
              let mut chunk_reader = reader.take(entry_info.size);
              let _ = io::copy(&mut chunk_reader, &mut writer)
                .inspect_err(|e| eprintln!("error decompressing: {e}"));
              o_reader = Some(chunk_reader.into_inner());
            }
            Err(e) => {
              eprintln!("error opening {}: {e}", out_file_name.to_string_lossy());
              continue;
            }
          }
        }
        Ok(())
      }
      Err(e) => Err(format!("error opening blob: {e}")),
    }
  }

  pub fn extract_all_files(&self, output_dir: &Path) -> Result<(), String> {
    self.entry_map.clone().into_par_iter().for_each(|(_, entry_info)| {
      if let Err(e) = self.extract_entry(entry_info, output_dir.to_path_buf()) {
        eprintln!("error while extracting: {e}. skipping")
      }
    });
    Ok(())
  }
}

pub fn uncompress_archive(
  archive_path: &Path,
  output_path: &Path,
  num_threads: u32,
) -> Result<(), String> {
  let blob_path =
    PathBuf::from(archive_path.to_string_lossy().to_string().replace(".s4a.db", ".s4a.blob"));
  let archive_reader = LocalS4ArchiveReader::from(archive_path, &blob_path)?;
  archive_reader.extract_all_files(output_path)?;
  Ok(())
}
