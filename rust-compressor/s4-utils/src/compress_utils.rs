use std::path::Path;
use std::{fs, io};

pub fn compress_lzma(file_path: &Path, out_path: &Path) -> Result<u64, String> {
  let fr = fs::File::open(file_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let fw = fs::File::create(out_path).map_err(|e| format!("at opening {out_path:?}: {e}"))?;
  // 128KB buffers: https://eklitzke.org/efficient-file-copying-on-linux
  let mut buf_reader = io::BufReader::with_capacity(128 * 1024, fr);
  let mut lzma_writer = lzma::LzmaWriter::new_compressor(fw, 9)
    .map_err(|e| format!("at initializing lzma compressor: {e}"))?;
  let write_size = io::copy(&mut buf_reader, &mut lzma_writer)
    .map_err(|e| format!("at compressing {file_path:?}: {e}"))?;
  lzma_writer.finish().map_err(|e| format!("at compressing {file_path:?}: {e}"))?;
  Ok(write_size)
}

pub fn compress_lzma_in_mem(file_path: &Path) -> Result<Vec<u8>, String> {
  let input_data = fs::read(file_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let output_data = lzma::compress(&input_data, 9)
    .map_err(|e| format!("at compressing {file_path:?}: {e}"))?;
  Ok(output_data)
}

pub fn uncompress_lzma(file_path: &Path, out_path: &Path) -> Result<u64, String> {
  let fr = fs::File::open(out_path).map_err(|e| format!("at opening {file_path:?}: {e}"))?;
  let fw = fs::File::create(out_path).map_err(|e| format!("at opening {out_path:?}: {e}"))?;
  let mut lzma_reader = lzma::LzmaReader::new_decompressor(fr)
    .map_err(|e| format!("at initializing lzma un-compressor: {e}"))?;
  let mut buf_writer = io::BufWriter::with_capacity(128 * 1024, fw);
  let write_size = io::copy(&mut lzma_reader, &mut buf_writer)
    .map_err(|e| format!("at un-compressing {file_path:?}: {e}"))?;
  Ok(write_size)
}
