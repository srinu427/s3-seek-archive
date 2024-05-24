mod compress_utils;

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::{fs, io};
use std::ops::Deref;
use walkdir::WalkDir;

pub fn compress_directory(
  dir_path: &Path,
  output_path: &Path,
  num_threads: u32,
) -> Result<(), String> {
  let tmp_dir_path = PathBuf::from(format!("{}_tmp_{}", output_path.display(), 0));
  fs::create_dir_all(&tmp_dir_path).map_err(|e| format!("error creating temp dir: {e}"))?;
  // Init output DB file
  let db_path = tmp_dir_path.join("index.db");
  if db_path.exists() {
    if db_path.is_file() {
      println!("{} already exists. replacing it", &db_path.display());
      fs::remove_file(&db_path).map_err(|e| format!("db remove failed: {e}"))?;
    } else {
      return Err(format!("{} already exists but is not a file", &db_path.display()));
    }
  }

  let conn = rusqlite::Connection::open(&db_path)
    .map_err(|e| format!("error creating temp db file: {e}"))?;
  conn
    .execute(
      "CREATE TABLE entry_list (name VARCHAR(2048), type VARCHAR(8), offset BIGINT, size BIGINT)",
      [],
    )
    .map_err(|e| format!("error creating mysql table: {e}"))?;

  let mut insert_row_stmt = Mutex::new(
    conn
      .prepare("INSERT INTO entry_list (name, type, offset, size) VALUES (?1, ?2, ?3, ?4)")
      .map_err(|e| format!("error preparing insert statement: {e}"))?,
  );

  let blob_writer = Mutex::new(io::BufWriter::with_capacity(
    128 * 1024,
    fs::File::create(&format!("{}.s4a_blob", output_path.display()))
      .map_err(|e| format!("error opening blob file for writing: {e}"))?,
  ));

  // Thread pool
  let t_pool = rayon::ThreadPoolBuilder::new()
    .num_threads(num_threads as usize)
    .build()
    .map_err(|e| format!("error initializing thread pool: {e}"))?;

  t_pool.scope(|s| {
    for entry in WalkDir::new(dir_path) {
      match entry {
        Ok(entry) => {
          if entry.path() != dir_path {
            let entry_name = entry.path().strip_prefix(dir_path).unwrap_or(entry.path());
            let temp_file_path = tmp_dir_path
              .join(format!("{}.xz", entry_name.display()).replace("/", ":").replace("\\", ":"));

            if entry.path().is_file() {
              s.spawn(|_| {
                if let Err(e) = compress_utils::compress_lzma(entry.path(), &temp_file_path) {
                  println!("ERROR compressing {entry_name:?}: {e}. Skipping it")
                } else {
                  match fs::File::open(&temp_file_path) {
                    Ok(fr) => {
                      let mut buf_reader = io::BufReader::with_capacity(128 * 1024, fr);
                      io::copy(&mut buf_reader, &mut blob_writer.lock().unwrap().deref()).unwrap();
                      insert_row_stmt
                        .lock()
                        .unwrap()
                        .execute((&entry_name.to_string_lossy(), "FILE", 0u64, 0u64))
                        .unwrap();
                    }
                    Err(e) => println!(
                      "can't open temp file {}: {e}. skipping it(externally modified?)",
                      temp_file_path.display()
                    ),
                  }
                  fs::remove_file(&temp_file_path).unwrap()
                }
              });
            } else if entry.path().is_dir() {
              insert_row_stmt
                .lock()
                .unwrap()
                .execute((&entry_name.to_string_lossy(), "FOLDER", 0u64, 0u64))
                .unwrap();
            }
          }
        }
        Err(e) => println!("error reading entry: {e}. skipping"),
      }
    }
  });
  insert_row_stmt.lock().unwrap().finalize().map_err(|e| format!("error finalizing index writes: {e:?}"))?;
  conn.close().map_err(|e| format!("error flushing index data: {e:?}"))?;

  fs::rename(db_path, format!("{}.db", output_path.display())).map_err(|e| format!("lol: {e}"))?;

  fs::remove_dir_all(&tmp_dir_path).map_err(|e| format!("error removing temp dir: {e}"))?;

  Ok(())
}
