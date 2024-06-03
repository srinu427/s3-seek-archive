use s4_utils::uncompress_archive;
use s4_utils::compress_directory;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

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
  /// Compress Mode: Output files' name.
  /// A data file <output_name>.s4a.blob and sqlite index <output_name>.s4a.db will be generated.
  #[arg(long, short = 'o')]
  output_path: PathBuf,
  /// Number of files to compress in parallel (excluding the main thread).
  /// one more thread will be used for IO
  #[arg(long, short = 't', default_value_t = 1)]
  thread_count: u32,
}

#[derive(Args)]
struct UncompressArgs {
  /// Input s4a.db file name
  ///
  /// Program expects a ".s4a.db" extension and ".s4a.blob" file to be in the same directory
  #[arg(long, short = 'i')]
  input_path: PathBuf,
  /// Uncompress Mode: Output directory name
  #[arg(long, short = 'o')]
  output_path: PathBuf,
  /// Number of files to uncompress in parallel (excluding the main thread).
  #[arg(long, short = 't', default_value_t = 1)]
  thread_count: u32,
}

fn main() {
  let args = AppArgs::parse();
  match args.command {
    AppCommands::Compress(compress_args) => {
      if let Err(e) = compress_directory(
        &compress_args.input_path,
        &compress_args.output_path,
        compress_args.thread_count,
      ) {
        eprintln!("{e}");
      }
    }
    AppCommands::Uncompress(uncompress_args) => {
      if let Err(e) = uncompress_archive(
        &uncompress_args.input_path,
        &uncompress_args.output_path,
        uncompress_args.thread_count,
      ) {
        eprintln!("{e}");
      }
    }
  }
}
