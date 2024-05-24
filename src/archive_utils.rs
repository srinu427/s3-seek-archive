mod compress_utils;

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
  let output_blob = PathBuf::from(format!("{}.s4a_blob", output_name.display()));
  let output_db = PathBuf::from(format!("{}.s4a_db", output_name.display()));

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
  conn
    .execute(
      "CREATE TABLE entry_list (name VARCHAR(2048), type VARCHAR(8), offset BIGINT, size BIGINT)",
      [],
    )
    .map_err(|e| format!("error creating mysql table: {e}"))?;

  let mut insert_row_stmt = conn
    .prepare("INSERT INTO entry_list (name, type, offset, size) VALUES (?1, ?2, ?3, ?4)")
    .map_err(|e| format!("error preparing insert statement: {e}"))?;

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
              .inspect_err(|e| println!("error adding {} to index: {e}", &msg.entry_name));
            offset += write_size;
          }
          Err(e) => {
            println!("can't open temp file {:?}: {e}. externally modified?. skipping it", temp_file)
          }
        }
        let _ =
          fs::remove_file(temp_file).inspect_err(|e| println!("error removing temp file: {e}"));
      } else {
        let _ = insert_row_stmt
          .execute((&msg.entry_name, "FOLDER", 0u64, 0u64))
          .inspect_err(|e| println!("error adding {} to index: {e}", &msg.entry_name));
      }
    } else {
      break;
    }
  }
  let _ = insert_row_stmt.finalize().map_err(|e| println!("error flushing data to index: {e}"));

  Ok(())
}

pub fn compress_directory(
  dir_path: &Path,
  output_path: &Path,
  num_threads: u32,
) -> Result<(), String> {
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
      writer_loop(output_path_owned, rx).inspect_err(|e| println!("writer thread init error: {e}"));
  });

  t_pool.scope(|s| {
    for entry in WalkDir::new(&dir_path) {
      if t_handle.is_finished() {
        println!("writer thread stopped. stopping");
        return;
      }
      match entry {
        Ok(entry) => {
          let entry_name = entry
            .path()
            .strip_prefix(dir_path)
            .unwrap_or(entry.path())
            .to_string_lossy()
            .to_string();
          if entry_name == "" {
            continue
          }
          if entry.path().is_file() {
            let temp_file_path = tmp_dir_path.join(entry_name.replace("\\", ":").replace("/", ":"));
            let tx_thread_owned = tx.clone();
            s.spawn(move |_| {
              let _ = compress_utils::compress_lzma(entry.path(), &temp_file_path)
                .inspect_err(|e| println!("error compressing {:?}: {e}", entry.path()))
                .map(|_| {
                  let _ = tx_thread_owned
                    .send(Some(WriteThreadInput { temp_file: Some(temp_file_path), entry_name }))
                    .inspect_err(|e| {
                      println!("error writing {:?} to archive: {e}", entry.path());
                    });
                });
            });
          } else if entry.path().is_dir() {
            let _ = tx
              .send(Some(WriteThreadInput { temp_file: None, entry_name }))
              .inspect_err(|e| println!("error writing {:?} to archive: {e}", entry.path()));
          }
        }
        Err(e) => println!("error reading entry: {e}. skipping it"),
      }
    }
  });

  let _ = tx.send(None).inspect_err(|e| println!("error stopping writer: {e}"));
  let _ = t_handle.join();

  fs::remove_dir_all(&tmp_dir_path).map_err(|e| format!("error removing temp dir: {e}"))?;
  Ok(())
}
