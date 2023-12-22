use crate::error::{Error, Result};
use crate::{Flush, Processor, Status};

/// `ZlibDecompressReader` is a struct that allows decompression of data using the Zlib format.
///
/// See [`GzipDecompressReader`](crate::gzip::GzipDecompressReader) to learn how to use.
pub type ZlibDecompressReader<R> = crate::io::ProcessorReader<ZlibDecompress, R>;
/// `ZlibCompressWriter` is a struct that allows compression of data using the Zlib format.
///
/// See [`GzipCompressWriter`](crate::gzip::GzipCompressWriter) to learn how to use.
pub type ZlibCompressWriter<W> = crate::io::ProcessorWriter<ZlibCompress, W>;

#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
/// `AsyncZlibDecompressReader` is a struct that allows decompression of data using the zlib format.
pub type AsyncZlibDecompressReader<R> = crate::io::AsyncProcessorReader<ZlibDecompress, R>;

#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
/// `AsyncZlibCompressWriter` is a struct that allows compression of data using the zlib format.
pub type AsyncZlibCompressWriter<W> = crate::io::AsyncProcessorWriter<ZlibCompress, W>;

/// Zlib compression processor
#[derive(Debug)]
pub struct ZlibCompress {
    inner: flate2::Compress,
}

impl Default for ZlibCompress {
    fn default() -> Self {
        Self::new(flate2::Compression::default())
    }
}

impl ZlibCompress {
    pub fn new(level: flate2::Compression) -> Self {
        Self {
            inner: flate2::Compress::new(level, true),
        }
    }
}

impl Processor for ZlibCompress {
    fn total_in(&self) -> u64 {
        self.inner.total_in()
    }
    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
    fn reset(&mut self) {
        self.inner.reset();
    }
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<Status> {
        let status = self
            .inner
            .compress(
                input,
                output,
                match flush {
                    Flush::Finish => flate2::FlushCompress::Finish,
                    Flush::None => flate2::FlushCompress::None,
                },
            )
            .map_err(|e| Error::CompressError(e.to_string()))?;
        match status {
            flate2::Status::BufError => Err(Error::CompressError("BufError".to_string())),
            flate2::Status::Ok => Ok(Status::Ok),
            flate2::Status::StreamEnd => Ok(Status::StreamEnd),
        }
    }
}

/// Zlib decompression processor
#[derive(Debug)]
pub struct ZlibDecompress {
    inner: flate2::Decompress,
}

impl Default for ZlibDecompress {
    fn default() -> Self {
        Self::new()
    }
}

impl ZlibDecompress {
    pub fn new() -> Self {
        Self {
            inner: flate2::Decompress::new(true),
        }
    }
}

impl Processor for ZlibDecompress {
    fn total_in(&self) -> u64 {
        self.inner.total_in()
    }
    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<Status> {
        match self
            .inner
            .decompress(input, output, flate2::FlushDecompress::None)
        {
            Ok(flate2::Status::Ok) => match flush {
                Flush::Finish => Err(Error::MoreDataRequired),
                Flush::None => Ok(Status::Ok),
            },
            Ok(flate2::Status::BufError) => Err(Error::DecompressError("BufError".to_string())),
            Ok(flate2::Status::StreamEnd) => Ok(Status::StreamEnd),
            Err(e) => Err(Error::DecompressError(e.to_string())),
        }
    }
    fn reset(&mut self) {
        self.inner.reset(true);
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::test_compress;
    use std::io::Write;

    use super::*;

    // #[test]
    // fn test_zlib_decompress() -> anyhow::Result<()> {
    //     let data = include_bytes!("../testfiles/pg2701.txt.zlib");
    //     let decompress = ZlibDecompress::new();

    //     crate::tests::test_decompress(decompress, &data[..])?;

    //     Ok(())
    // }

    #[test]
    fn test_zlib_compress() -> anyhow::Result<()> {
        test_compress(
            || Ok(ZlibCompress::new(flate2::Compression::default())),
            || Ok(ZlibDecompress::new()),
        )
    }

    #[test]
    fn create_zlib_test_file() -> anyhow::Result<()> {
        let mut out = std::fs::File::create("target/pg2701.txt.zlib")?;
        let data = include_bytes!("../testfiles/pg2701.txt");
        let mut writer = flate2::write::ZlibEncoder::new(&mut out, flate2::Compression::default());
        writer.write_all(data)?;
        Ok(())
    }
}
