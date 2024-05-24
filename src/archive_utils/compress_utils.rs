use std::path::Path;
use std::{fs, io};

pub fn compress_lzma(file_path: &Path, out_path: &Path) -> Result<u64, String> {
  match fs::File::open(file_path) {
    Ok(fr) => match fs::File::create(out_path) {
      Ok(fw) => {
        // 128KB buffers: https://eklitzke.org/efficient-file-copying-on-linux
        let mut buf_reader = io::BufReader::with_capacity(128 * 1024, fr);
        let mut lzma_writer =
          lzma::LzmaWriter::with_capacity(128 * 1024, fw, lzma::Direction::Compress, 9)
            .map_err(|e| format!("error initializing lzma compressor: {e}"))?;
        let write_size = io::copy(&mut buf_reader, &mut lzma_writer)
          .map_err(|e| format!("error compressing {file_path:?}: {e}"))?;
        lzma_writer.finish().map_err(|e| format!("error compressing {file_path:?}: {e}"))?;
        Ok(write_size)
      }
      Err(e) => return Err(format!("error opening {out_path:?}: {e}")),
    },
    Err(e) => return Err(format!("error opening {file_path:?}: {e}")),
  }
}
