use clap::{Args, Parser, Subcommand};
use s4_utils::compress_directory;
use s4_utils::mux_db_and_blob;
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
  Decompress(DecompressArgs),
  Mux(MuxArgs),
  Demux(DemuxArgs),
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
  /// Mux the s4a.db and s4a.blob files to a single s4a file
  /// Useful if mux takes too long
  #[arg(long, short = 'm')]
  mux: bool,
  /// Max size of file in bytes to be processed in memory instead of writing to temp file.
  /// Use 0 to reduce RAM usage
  #[arg(long, short = 'M', default_value_t = 8 * 1024 * 1024)]
  max_in_mem_file_size: u64,
  /// Set write buffer size
  #[arg(long, short = 'W', default_value_t = 32 * 1024 * 1024)]
  write_buffer_size: u64,
}

#[derive(Args)]
struct DecompressArgs {
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

#[derive(Args)]
struct MuxArgs {
  /// Input s4a.db file name
  #[arg(long, short = 'i')]
  input_path: PathBuf,
}

#[derive(Args)]
struct DemuxArgs {
  /// Input s4a file name
  #[arg(long, short = 'i')]
  input_path: PathBuf,
}

fn main() {
  let args = AppArgs::parse();
  match args.command {
    AppCommands::Compress(mut compress_args) => {
      if !compress_args.output_path.ends_with(".s4a") {
        compress_args.output_path = compress_args.output_path.with_extension("s4a");
      }
      if let Err(e) = compress_directory(
        &compress_args.input_path,
        &compress_args.output_path,
        compress_args.thread_count,
        CompressionType::from_str(&compress_args.compression).expect("shouldn't happen"),
        compress_args.mux,
        compress_args.max_in_mem_file_size,
        compress_args.write_buffer_size,
      ) {
        eprintln!("{e}");
      }
    }
    AppCommands::Decompress(uncompress_args) => {
      if let Err(e) = uncompress_archive(
        &uncompress_args.input_path,
        &uncompress_args.output_path,
        uncompress_args.thread_count,
        &uncompress_args.pattern
      ) {
        eprintln!("{e}");
      }
    }
    AppCommands::Mux(mux_args) => {
      if !mux_args.input_path.ends_with(".s4a.db") {
        eprintln!("expected file with extension of .s4a.db");
        return;
      }
      let blob_path = mux_args.input_path.with_extension(".blob");
      if let Err(e) = mux_db_and_blob(
        &mux_args.input_path,
        &blob_path,
      ) {
        eprintln!("{e}");
      }
    },
    AppCommands::Demux(demux_args) => {
      if !demux_args.input_path.ends_with(".s4a") {
        eprintln!("expected file with extension of .s4a");
        return;
      }
    },
  }
}
