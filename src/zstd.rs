//! Zstandard format support

use zstd::stream::raw::Operation;
use zstd::zstd_safe::{InBuffer, OutBuffer};

use crate::error::Result;
use crate::{Error, Flush, Processor, Status};

/// `ZstdDecompressReader` is a struct that allows decompression of data using the Zstandard format.
///
/// See [`GzipDecompressReader`](crate::gzip::GzipDecompressReader) to learn how to use.
pub type ZstdDecompressReader<R> = crate::io::ProcessorReader<ZstdDecompress, R>;
/// `ZstdCompressWriter` is a struct that allows compression of data using the Zstandard format.
///
/// See [`GzipCompressWriter`](crate::gzip::GzipCompressWriter) to learn how to use.
pub type ZstdCompressWriter<W> = crate::io::ProcessorWriter<ZstdCompress, W>;

/// `AsyncZstdDecompressReader` is a struct that allows decompression of data using the zstd format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncReadExt;
/// # use tokio::io::BufReader;
/// # use tokio::fs::File;
/// # use autocompress::zstd::AsyncZstdDecompressReader;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.zst").await?);
/// let mut zstd_reader = AsyncZstdDecompressReader::new(buf_reader);
/// let mut buf = Vec::new();
/// zstd_reader.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncZstdDecompressReader<R> = crate::io::AsyncProcessorReader<ZstdDecompress, R>;

/// `AsyncZstdCompressWriter` is a struct that allows compression of data using the zstd format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncWriteExt;
/// # use tokio::fs::File;
/// # use autocompress::zstd::AsyncZstdCompressWriter;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/doc-zstd-compress-async-writer.zst").await?;
/// let mut zstd_writer = AsyncZstdCompressWriter::new(file_writer);
/// zstd_writer.write_all(&b"Hello, world"[..]).await?;
/// zstd_writer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncZstdCompressWriter<W> = crate::io::AsyncProcessorWriter<ZstdCompress, W>;

/// Zstandard compression processor
pub struct ZstdCompress {
    inner: zstd::stream::raw::Encoder<'static>,
    total_in: u64,
    total_out: u64,
}

impl Default for ZstdCompress {
    fn default() -> Self {
        Self::new(crate::CompressionLevel::default().zstd())
            .expect("Failed to initialize zstd encoder")
    }
}

impl ZstdCompress {
    pub fn new(level: i32) -> Result<Self> {
        Ok(Self {
            inner: zstd::stream::raw::Encoder::new(level)?,
            total_in: 0,
            total_out: 0,
        })
    }
}

impl Processor for ZstdCompress {
    fn total_in(&self) -> u64 {
        self.total_in
    }
    fn total_out(&self) -> u64 {
        self.total_out
    }
    fn process(&mut self, input: &[u8], output: &mut [u8], flush: Flush) -> Result<Status> {
        let mut input_buf = InBuffer::around(input);
        let mut output_buf = OutBuffer::around(output);

        let mut remain_bytes = usize::MAX;

        self.inner.run(&mut input_buf, &mut output_buf)?;
        while output_buf.pos() < output_buf.capacity() && flush == Flush::Finish && remain_bytes > 0
        {
            remain_bytes = self.inner.finish(&mut output_buf, true)?;
        }

        self.total_in += TryInto::<u64>::try_into(input_buf.pos()).unwrap();
        self.total_out += TryInto::<u64>::try_into(output_buf.pos()).unwrap();

        match flush {
            Flush::Finish => {
                if remain_bytes > 0 {
                    Ok(Status::Ok)
                } else {
                    Ok(Status::StreamEnd)
                }
            }
            Flush::None => Ok(Status::Ok),
        }
    }
    fn reset(&mut self) {
        self.inner.reinit().expect("Failed to reset zstd encoder");
        self.total_in = 0;
        self.total_out = 0;
    }
}

/// Zstandard decompression processor
pub struct ZstdDecompress {
    inner: zstd::stream::raw::Decoder<'static>,
    total_in: u64,
    total_out: u64,
}

impl ZstdDecompress {
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: zstd::stream::raw::Decoder::new()?,
            total_in: 0,
            total_out: 0,
        })
    }
}

impl Default for ZstdDecompress {
    fn default() -> Self {
        Self::new().expect("Failed to initialize zstd decoder")
    }
}

impl Processor for ZstdDecompress {
    fn total_in(&self) -> u64 {
        self.total_in
    }
    fn total_out(&self) -> u64 {
        self.total_out
    }
    fn process(&mut self, mut input: &[u8], mut output: &mut [u8], flush: Flush) -> Result<Status> {
        loop {
            let status = self.inner.run_on_buffers(input, output)?;
            // eprintln!(
            //     "decompress {} {} {}",
            //     status.bytes_read, status.bytes_written, status.remaining
            // );
            self.total_in += TryInto::<u64>::try_into(status.bytes_read).unwrap();
            self.total_out += TryInto::<u64>::try_into(status.bytes_written).unwrap();
            if status.remaining == 0 {
                // eprintln!("stream end");
                return Ok(Status::StreamEnd);
            }

            if status.bytes_read < input.len() && status.bytes_written < output.len() {
                input = &input[status.bytes_read..];
                output = &mut output[status.bytes_written..];
            } else {
                break;
            }
        }

        match flush {
            Flush::Finish => Err(Error::MoreDataRequired),
            Flush::None => Ok(Status::Ok),
        }
    }
    fn reset(&mut self) {
        self.inner.reinit().expect("Failed to reset zstd decoder");
        self.total_in = 0;
        self.total_out = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::tests::test_compress;
    use anyhow::Context;

    #[test]
    fn test_zstd_decompress() -> anyhow::Result<()> {
        let data = include_bytes!("../testfiles/sqlite3.c.zst");
        let decompress = ZstdDecompress::new()?;

        crate::tests::test_decompress(decompress, &data[..])?;

        Ok(())
    }

    #[test]
    fn test_zstd_compress() -> anyhow::Result<()> {
        test_compress(
            || ZstdCompress::new(10).context("zstd compress"),
            || ZstdDecompress::new().context("zstd decompress"),
        )
    }

    #[test]
    fn test_zstd_reader() -> anyhow::Result<()> {
        let expected_data = include_bytes!("../testfiles/sqlite3.c");
        let data = include_bytes!("../testfiles/sqlite3.c.zst");
        let mut decompressed_data = Vec::new();
        let mut reader = ZstdDecompressReader::new(&data[..]);
        reader.read_to_end(&mut decompressed_data)?;
        assert_eq!(decompressed_data.len(), expected_data.len());
        assert_eq!(decompressed_data, expected_data);

        Ok(())
    }
}
