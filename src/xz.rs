use std::sync::atomic::AtomicU64;

use crate::{CompressionLevel, Error, Flush, Processor, Result};
use once_cell::sync::Lazy;
use xz2::stream::Status;

pub const DEFAULT_XZ_MEM_SIZE: u64 = 1_000_000_000;
static DECOMPRESS_XZ_MEM_SIZE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(DEFAULT_XZ_MEM_SIZE));

pub fn set_default_decompress_mem_size(mem_size: u64) {
    DECOMPRESS_XZ_MEM_SIZE.swap(mem_size, std::sync::atomic::Ordering::Relaxed);
}

/// `XzCompressWriter` is a struct that allows compression of data using the Xz format.
///
/// See [`GzipCompressWriter`](crate::gzip::GzipCompressWriter) to learn how to use.
pub type XzCompressWriter<W> = crate::io::ProcessorWriter<XzCompress, W>;
/// `XzDecompressReader` is a struct that allows decompression of data using the Xz format.
///
/// See [`GzipDecompressReader`](crate::gzip::GzipDecompressReader) to learn how to use.
pub type XzDecompressReader<R> = crate::io::ProcessorReader<XzDecompress, R>;

/// `AsyncXzDecompressReader` is a struct that allows decompression of data using the Xz format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncReadExt;
/// # use tokio::io::BufReader;
/// # use tokio::fs::File;
/// # use autocompress::xz::AsyncXzDecompressReader;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.xz").await?);
/// let mut xz_reader = AsyncXzDecompressReader::new(buf_reader);
/// let mut buf = Vec::new();
/// xz_reader.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncXzDecompressReader<R> = crate::io::AsyncProcessorReader<XzDecompress, R>;

/// `AsyncXzCompressWriter` is a struct that allows compression of data using the Xz format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncWriteExt;
/// # use tokio::fs::File;
/// # use autocompress::xz::AsyncXzCompressWriter;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/doc-xz-compress-async-writer.gz").await?;
/// let mut xz_writer = AsyncXzCompressWriter::new(file_writer);
/// xz_writer.write_all(&b"Hello, world"[..]).await?;
/// xz_writer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncXzCompressWriter<W> = crate::io::AsyncProcessorWriter<XzCompress, W>;

/// XZ compression processor
pub struct XzCompress {
    inner: xz2::stream::Stream,
    preset: u32,
}

impl XzCompress {
    pub fn new(preset: u32) -> Result<Self> {
        let inner = xz2::stream::Stream::new_easy_encoder(preset, xz2::stream::Check::Crc64)
            .map_err(|e| Error::CompressError(e.to_string()))?;
        Ok(XzCompress { inner, preset })
    }
}

impl Default for XzCompress {
    fn default() -> Self {
        Self::new(CompressionLevel::default().xz()).unwrap()
    }
}

impl Processor for XzCompress {
    fn total_in(&self) -> u64 {
        self.inner.total_in()
    }
    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<crate::Status> {
        match self.inner.process(
            input,
            output,
            match flush {
                Flush::Finish => xz2::stream::Action::Finish,
                Flush::None => xz2::stream::Action::Run,
            },
        ) {
            Ok(Status::Ok) | Ok(Status::MemNeeded) => Ok(crate::Status::Ok),
            Ok(Status::GetCheck) => Err(Error::DecompressError("GetCheck".to_string())),
            Ok(Status::StreamEnd) => Ok(crate::Status::StreamEnd),
            Err(e) => Err(Error::DecompressError(e.to_string())),
        }
    }
    fn reset(&mut self) {
        self.inner =
            xz2::stream::Stream::new_easy_encoder(self.preset, xz2::stream::Check::Crc64).unwrap();
    }
}

/// Xz decompression processor
pub struct XzDecompress {
    memlimit: u64,
    inner: xz2::stream::Stream,
}

impl XzDecompress {
    pub fn new(memlimit: u64) -> Result<Self> {
        Ok(Self {
            memlimit,
            inner: xz2::stream::Stream::new_stream_decoder(memlimit, xz2::stream::TELL_NO_CHECK)
                .map_err(|e| Error::CompressError(e.to_string()))?,
        })
    }
}

impl Default for XzDecompress {
    fn default() -> Self {
        Self::new(DECOMPRESS_XZ_MEM_SIZE.load(std::sync::atomic::Ordering::Relaxed)).unwrap()
    }
}

impl Processor for XzDecompress {
    fn total_in(&self) -> u64 {
        self.inner.total_in()
    }
    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<crate::Status> {
        match self.inner.process(input, output, xz2::stream::Action::Run) {
            Ok(Status::Ok) | Ok(Status::MemNeeded) => match flush {
                Flush::Finish => Err(Error::MoreDataRequired),
                Flush::None => Ok(crate::Status::Ok),
            },
            Ok(Status::GetCheck) => Err(Error::DecompressError("GetCheck".to_string())),
            //Ok(Status::MemNeeded) => Err(Error::DecompressError("MemNeeded".to_string())),
            Ok(Status::StreamEnd) => Ok(crate::Status::StreamEnd),
            Err(e) => Err(Error::DecompressError(e.to_string())),
        }
    }
    fn reset(&mut self) {
        self.inner =
            xz2::stream::Stream::new_stream_decoder(self.memlimit, xz2::stream::TELL_NO_CHECK)
                .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_compress;
    use crate::tests::test_decompress;
    use anyhow::Context;

    #[test]
    fn test_xz_decompress() -> anyhow::Result<()> {
        let data = include_bytes!("../testfiles/sqlite3.c.xz");
        let decompress = XzDecompress::new(10_000_000)?;

        test_decompress(decompress, &data[..])?;

        Ok(())
    }

    #[test]
    fn test_xz() -> crate::error::Result<()> {
        let data = include_bytes!("../testfiles/sqlite3.c.xz");
        test_decompress(XzDecompress::new(10_000_000)?, data)?;
        Ok(())
    }

    #[test]
    fn test_xz_multistream() -> crate::error::Result<()> {
        let data = include_bytes!("../testfiles/sqlite3.c.multistream.xz");
        test_decompress(XzDecompress::new(10_000_000)?, data)?;
        Ok(())
    }

    #[test]
    fn test_xz_compress() -> anyhow::Result<()> {
        test_compress(
            || XzCompress::new(6).context("zstd compress"),
            || XzDecompress::new(10_000_000).context("zstd decompress"),
        )
    }
}
