use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::{fs, io};

#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
  LZMA = 1,
  LZ4 = 2,
}

impl FromStr for CompressionType {
  type Err = ();

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "LZMA" | "lzma" => Ok(CompressionType::LZMA),
      "LZ4" | "lz4" => Ok(CompressionType::LZ4),
      _ => Ok(CompressionType::LZ4),
    }
  }
}

impl ToString for CompressionType {
  fn to_string(&self) -> String {
    match self {
      CompressionType::LZMA => "LZMA".to_string(),
      CompressionType::LZ4 => "LZ4".to_string(),
    }
  }
}

// LZMA

pub fn _uncompress_lzma(file_path: &Path, out_path: &Path) -> Result<u64, String> {
  let fr = fs::File::open(out_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let fw = fs::File::create(out_path).map_err(|e| format!("at opening {out_path:?}: {e}"))?;
  let mut lzma_reader = lzma::LzmaReader::new_decompressor(fr)
    .map_err(|e| format!("at initializing lzma un-compressor: {e}"))?;
  let mut buf_writer = io::BufWriter::with_capacity(128 * 1024, fw);
  let write_size = io::copy(&mut lzma_reader, &mut buf_writer)
    .map_err(|e| format!("at un-compressing {file_path:?}: {e}"))?;
  Ok(write_size)
}

pub fn compress(
  file_path: &Path,
  out_path: &Path,
  compression: CompressionType
) -> Result<u64, String> {
  let fr = fs::File::open(file_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let fw = fs::File::create(out_path).map_err(|e| format!("at opening {out_path:?}: {e}"))?;
  // 128KB buffers: https://eklitzke.org/efficient-file-copying-on-linux
  let mut buf_reader = io::BufReader::with_capacity(128 * 1024, fr);
  let write_size = match compression {
    CompressionType::LZMA => {
      let mut lzma_writer = lzma::LzmaWriter::new_compressor(fw, 9)
        .map_err(|e| format!("at initializing lzma compressor: {e}"))?;
      let out_size = io::copy(&mut buf_reader, &mut lzma_writer)
        .map_err(|e| format!("at compressing {file_path:?}: {e}"))?;
      lzma_writer.finish()
        .map_err(|e| format!("at flushing to {file_path:?}: {e}"))?;
      out_size
    },
    CompressionType::LZ4 => {
      let mut lz4_writer = lz4_flex::frame::FrameEncoder::new(fw);
      let out_size = io::copy(&mut buf_reader, &mut lz4_writer)
        .map_err(|e| format!("at compressing {file_path:?}: {e}"))?;
      lz4_writer.flush()
        .map_err(|e| format!("at flushing to {file_path:?}: {e}"))?;
      out_size
    },
  };
  Ok(write_size)
}

pub fn compress_in_mem(file_path: &Path, compression: CompressionType) -> Result<Vec<u8>, String> {
  let input_data = fs::read(file_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let output_data = match compression {
    CompressionType::LZMA => {
      lzma::compress(&input_data, 9).map_err(|e| format!("at compressing {file_path:?}: {e}"))?
    }
    CompressionType::LZ4 => {
      let compressed_data = Vec::with_capacity(32 * 1024 * 1024);
      let mut lz4_writer = lz4_flex::frame::FrameEncoder::new(compressed_data);
      lz4_writer.write_all(&input_data).map_err(|e| format!("at compressing {file_path:?}: {e}"))?;
      lz4_writer.finish().map_err(|e| format!("at compressing {file_path:?}: {e}"))?
    },
  };
  Ok(output_data)
}

pub fn decompress(file_path: &Path, out_path: &Path, compression: CompressionType) -> Result<u64, String> {
  let fr = fs::File::open(out_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let fw = fs::File::create(out_path).map_err(|e| format!("at opening {out_path:?}: {e}"))?;
  let mut buf_writer = io::BufWriter::with_capacity(128 * 1024, fw);
  let write_size = match compression {
    CompressionType::LZMA => {
      let mut lzma_reader = lzma::LzmaReader::new_decompressor(fr)
        .map_err(|e| format!("at initializing lzma un-compressor: {e}"))?;
      let out_size = io::copy(&mut lzma_reader, &mut buf_writer)
        .map_err(|e| format!("at un-compressing {file_path:?}: {e}"))?;
      buf_writer
        .flush()
        .map_err(|e| format!("at flushing to {out_path:?}: {e}"))?;
      out_size
    },
    CompressionType::LZ4 => {
      let mut lz4_reader = lz4_flex::frame::FrameDecoder::new(fr);
      let out_size = io::copy(&mut lz4_reader, &mut buf_writer)
        .map_err(|e| format!("at un-compressing {file_path:?}: {e}"))?;
      buf_writer
        .flush()
        .map_err(|e| format!("at flushing to {out_path:?}: {e}"))?;
      out_size
    },
  };
  Ok(write_size)
}

pub fn decompress_stream<R: io::Read, W: io::Write>(inp: R, out: &mut W, compression: CompressionType) -> Result<u64, String> {
  let write_size = match compression {
    CompressionType::LZMA => {
      let mut lzma_reader = lzma::LzmaReader::new_decompressor(inp)
        .map_err(|e| format!("at initializing lzma un-compressor: {e}"))?;
      let out_size = io::copy(&mut lzma_reader, out)
        .map_err(|e| format!("at un-compressing stream: {e}"))?;
      out
        .flush()
        .map_err(|e| format!("at flushing stream: {e}"))?;
      out_size
    },
    CompressionType::LZ4 => {
      let mut lz4_reader = lz4_flex::frame::FrameDecoder::new(inp);
      let out_size = io::copy(&mut lz4_reader, out)
        .map_err(|e| format!("at un-compressing stream: {e}"))?;
      out
        .flush()
        .map_err(|e| format!("at flushing to stream: {e}"))?;
      out_size
    },
  };
  Ok(write_size)
}

pub fn decompress_from_mem(compressed_data: &[u8], compression: CompressionType) -> Result<Vec<u8>, String> {
  let output_data = match compression {
    CompressionType::LZMA => {
      lzma::decompress(compressed_data).map_err(|e| format!("at decompressing: {e}"))?
    },
    CompressionType::LZ4 => {
      let mut lz4_reader = lz4_flex::frame::FrameDecoder::new(compressed_data);
      let mut decompressed_data = vec![];
      lz4_reader
        .read_to_end(&mut decompressed_data)
        .map_err(|e| format!("at decompressing: {e}"))?;
      decompressed_data
    },
  };
  Ok(output_data)
}
