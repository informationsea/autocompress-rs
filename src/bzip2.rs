//! Bzip2 format support

use crate::{Error, Flush, Processor, Result, Status};

/// `Bzip2DecompressReader` is a struct that allows decompression of data using the BZIP2 format.
///
/// See [`GzipDecompressReader`](crate::gzip::GzipDecompressReader) to learn how to use.
pub type Bzip2DecompressReader<R> = crate::io::ProcessorReader<Bzip2Decompress, R>;
/// `Bzip2CompressWriter` is a struct that allows compression of data using the BGZIP2 format.
///
/// See [`GzipCompressWriter`](crate::gzip::GzipCompressWriter) to learn how to use.
pub type Bzip2CompressWriter<W> = crate::io::ProcessorWriter<Bzip2Compress, W>;

/// `AsyncBzip2DecompressReader` is a struct that allows decompression of data using the BZip2 format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncReadExt;
/// # use tokio::io::BufReader;
/// # use tokio::fs::File;
/// # use autocompress::bzip2::AsyncBzip2DecompressReader;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/pg2701.txt.bz2").await?);
/// let mut bzip2_reader = AsyncBzip2DecompressReader::new(buf_reader);
/// let mut buf = Vec::new();
/// bzip2_reader.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncBzip2DecompressReader<R> = crate::io::AsyncProcessorReader<Bzip2Decompress, R>;

/// `AsyncBzip2CompressWriter` is a struct that allows compression of data using the BZip2 format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncWriteExt;
/// # use tokio::fs::File;
/// # use autocompress::bzip2::AsyncBzip2CompressWriter;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/doc-bzip2-compress-async-writer.gz").await?;
/// let mut bzip2_writer = AsyncBzip2CompressWriter::new(file_writer);
/// bzip2_writer.write_all(&b"Hello, world"[..]).await?;
/// bzip2_writer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncBzip2CompressWriter<W> = crate::io::AsyncProcessorWriter<Bzip2Compress, W>;

/// Bzip2 compression processor
pub struct Bzip2Compress {
    inner: bzip2::Compress,
    compression: bzip2::Compression,
    work_factor: u32,
}

const DEFAULT_WORK_FACTOR: u32 = 30;

impl Bzip2Compress {
    pub fn new(compression: bzip2::Compression) -> Self {
        Self {
            inner: bzip2::Compress::new(compression, DEFAULT_WORK_FACTOR),
            compression,
            work_factor: DEFAULT_WORK_FACTOR,
        }
    }
}

impl Default for Bzip2Compress {
    fn default() -> Self {
        Self::new(bzip2::Compression::default())
    }
}

impl Processor for Bzip2Compress {
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<Status> {
        match self.inner.compress(
            input,
            output,
            match flush {
                Flush::Finish => bzip2::Action::Finish,
                Flush::None => bzip2::Action::Run,
            },
        ) {
            Err(e) => Err(Error::DecompressError(e.to_string())),
            Ok(bzip2::Status::Ok) => Ok(Status::Ok),
            Ok(bzip2::Status::StreamEnd) => Ok(Status::StreamEnd),
            Ok(bzip2::Status::MemNeeded) => Ok(Status::Ok),
            Ok(bzip2::Status::FinishOk) => Ok(Status::Ok),
            Ok(bzip2::Status::RunOk) => Ok(Status::Ok),
            Ok(bzip2::Status::FlushOk) => Ok(Status::Ok),
        }
    }

    fn reset(&mut self) {
        self.inner = bzip2::Compress::new(self.compression, self.work_factor);
    }

    fn total_in(&self) -> u64 {
        self.inner.total_in()
    }

    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
}

/// Bzip2 decompression processor
pub struct Bzip2Decompress {
    inner: bzip2::Decompress,
}

impl Bzip2Decompress {
    pub fn new() -> Self {
        Self {
            inner: bzip2::Decompress::new(false),
        }
    }
}

impl Default for Bzip2Decompress {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for Bzip2Decompress {
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<Status> {
        match self.inner.decompress(input, output) {
            Err(e) => Err(Error::DecompressError(e.to_string())),
            Ok(bzip2::Status::Ok) | Ok(bzip2::Status::MemNeeded) => match flush {
                Flush::Finish => Err(Error::MoreDataRequired),
                Flush::None => Ok(Status::Ok),
            },
            Ok(bzip2::Status::StreamEnd) => Ok(Status::StreamEnd),
            _ => unreachable!(),
        }
    }

    fn reset(&mut self) {
        self.inner = bzip2::Decompress::new(false);
    }

    fn total_in(&self) -> u64 {
        self.inner.total_in()
    }

    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_compress;
    use crate::tests::test_decompress;

    #[test]
    fn test_bzip2_decompress() -> anyhow::Result<()> {
        let data = include_bytes!("../testfiles/pg2701.txt.bz2");
        let decompress = Bzip2Decompress::new();
        test_decompress(decompress, &data[..])?;

        Ok(())
    }

    #[test]
    fn test_bzip2() -> crate::error::Result<()> {
        let data = include_bytes!("../testfiles/pg2701.txt.bz2");
        test_decompress(Bzip2Decompress::new(), data)?;
        Ok(())
    }

    #[test]
    fn test_bzip2_multistream() -> crate::error::Result<()> {
        let data = include_bytes!("../testfiles/pg2701.txt.multistream.bz2");
        test_decompress(Bzip2Decompress::new(), data)?;
        Ok(())
    }

    #[test]
    fn test_bzip2_compress() -> anyhow::Result<()> {
        test_compress(
            || Ok(Bzip2Compress::new(bzip2::Compression::default())),
            || Ok(Bzip2Decompress::new()),
        )
    }
}
