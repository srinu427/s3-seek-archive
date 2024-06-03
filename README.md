# s3-seek-archive


An archival tool to make handling directories with huge number of small files in S3 better.

## Features

- Reduce number of files to be uploaded to S3 by archiving them to 2 files
- Reduce the storage used in S3 without losing the ability to get individual files from S3
- Designed with source code and coverage reports as a focus, but can be used with any directories with large number of small files
- Multiplatform support - The tool is compatible with Linux, macOS and Windows


## Download

Available releases can be downloaded for your platform of choice on the [Releases](https://github.com/zaszi/rust-template/releases) page. These are merely provided as an example on how the asset uploading works, and aren't actually useful by themselves beyond what a `hello world` program can provide.

## Usage

Compressing a directory

```
Usage: s3-seek-archive.exe compress [OPTIONS] --input-path <INPUT_PATH> --output-path <OUTPUT_PATH>

Options:
  -i, --input-path <INPUT_PATH>      Input directory name. If a file is provided, empty archive is generated
  -o, --output-path <OUTPUT_PATH>    Compress Mode: Output files' name. A data file <output_name>.s4a.blob and sqlite index <output_name>.s4a.db will be generated
  -t, --thread-count <THREAD_COUNT>  Number of files to compress in parallel (excluding the main thread). one more thread will be used for IO [default: 1]
  -h, --help                         Print help
  -V, --version                      Print version
```

## Building

If desired, you can build s3-seek-archive yourself. You will need a working `Rust` and `Cargo` setup. [Rustup](https://rustup.rs/) is the simplest way to set this up on either Windows, Mac or Linux.

Once the prerequisites have been installed, compilation on your native platform is as simple as running the following in a terminal:

```
cd rust-compressor
cargo build --release
```

## WebAssembly

No web assembly support since we need file IO

## Contribution

Found a problem or have a suggestion? Feel free to open an issue.

## License

idk