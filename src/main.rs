mod archive_utils;

use archive_utils::compress_directory;
use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct AppArgs{
  /// Output directory name.
  /// If a file is given, it will be ignored.
  /// There is no benefit of using this tool instead of lzma for a single file
  #[arg(long, short = 'i')]
  input_dir: PathBuf,
  /// Output files' name.
  /// A data file <output_name>.s4a.blob and sqlite index <output_name>.s4a.db will be generated.
  #[arg(long, short = 'o')]
  output_name: PathBuf,
  /// Number of files to compress in parallel.
  /// 2 more threads will be used to the number given here for the main thread and output io thread
  #[arg(long, short = 't', default_value_t = 1)]
  thread_count: u32
}

fn main() {
  let args = AppArgs::parse();
  if let Err(e) = compress_directory(&args.input_dir, &args.output_name, args.thread_count) {
    println!("{e}");
  }
}
