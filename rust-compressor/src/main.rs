use clap::{Args, Parser, Subcommand};
use s4_utils::compress_directory;
use s4_utils::uncompress_archive;
use s4_utils::CompressionType;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct AppArgs {
  #[command(subcommand)]
  command: AppCommands,
}

#[derive(Subcommand)]
enum AppCommands {
  Compress(CompressArgs),
  Uncompress(UncompressArgs),
}

#[derive(Args)]
struct CompressArgs {
  /// Input directory name. If a file is provided, empty archive is generated
  #[arg(long, short = 'i')]
  input_path: PathBuf,
  /// Compress Mode: Output file's name.
  /// An s4a file <output_name>.s4a will be generated.
  #[arg(long, short = 'o')]
  output_path: PathBuf,
  /// Number of files to compress in parallel (excluding the main thread).
  /// one more thread will be used for IO
  #[arg(long, short = 't', default_value_t = 1)]
  thread_count: u32,
  /// Compression to use. Defaults to LZ4
  /// supported: LZMA, LZ4
  #[arg(long, short = 'c', default_value_t = String::from("LZ4"))]
  compression: String,
  /// Max size of file in bytes to be processed in memory instead of writing to temp file.
  /// Use 0 to reduce RAM usage
  #[arg(long, short = 'M', default_value_t = 8 * 1024 * 1024)]
  max_in_mem_file_size: u64,
  /// Set write buffer size
  #[arg(long, short = 'W', default_value_t = 32 * 1024 * 1024)]
  write_buffer_size: u64,
}

#[derive(Args)]
struct UncompressArgs {
  /// Input s4a file name
  #[arg(long, short = 'i')]
  input_path: PathBuf,
  /// Uncompress Mode: Output directory name
  #[arg(long, short = 'o')]
  output_path: PathBuf,
  /// NOTE: currently ignored. Number of files to uncompress in parallel (excluding the main thread).
  #[arg(long, short = 't', default_value_t = 1)]
  thread_count: u32,
  /// regex pattern of files to extract
  #[arg(long, short = 'p', default_value_t = String::from(".*"))]
  pattern: String,
}

fn main() {
  let args = AppArgs::parse();
  match args.command {
    AppCommands::Compress(compress_args) => {
      if let Err(e) = compress_directory(
        &compress_args.input_path,
        &compress_args.output_path,
        compress_args.thread_count,
        CompressionType::from_str(&compress_args.compression).expect("shouldn't happen"),
        compress_args.max_in_mem_file_size,
        compress_args.write_buffer_size,
      ) {
        eprintln!("{e}");
      }
    }
    AppCommands::Uncompress(uncompress_args) => {
      if let Err(e) = uncompress_archive(
        &uncompress_args.input_path,
        &uncompress_args.output_path,
        uncompress_args.thread_count,
        &uncompress_args.pattern
      ) {
        eprintln!("{e}");
      }
    }
  }
}
