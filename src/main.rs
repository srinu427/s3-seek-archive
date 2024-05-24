mod archive_utils;

use archive_utils::compress_directory;
use std::path::PathBuf;

fn main() {
  if let Err(e) = compress_directory(&PathBuf::from("."), &PathBuf::from("archive"), 4) {
    println!("{e}");
  }
}
