//! gzip format support
//!
//! Gzip format support is provided by [flate2](https://github.com/rust-lang/flate2-rs)
mod footer;
mod header;

pub use footer::*;
pub use header::*;

use crate::error::{Error, Result};
use crate::{Flush, Processor, Status};

/// `GzipDecompressReader` is a struct that allows decompression of data using the GZIP format.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::io::BufReader;
/// # use std::fs::File;
/// use autocompress::gzip::GzipDecompressReader;
///
/// # fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.gz")?);
/// let mut gzip_reader = GzipDecompressReader::new(buf_reader);
/// let mut buf = Vec::new();
/// gzip_reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub type GzipDecompressReader<R> = crate::io::ProcessorReader<GzipDecompress, R>;

/// `GzipCompressWriter` is a struct that allows compression of data using the GZIP format.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::fs::File;
/// use autocompress::gzip::GzipCompressWriter;
///
/// # fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/doc-gzip-compress-writer.gz")?;
/// let mut gzip_writer = GzipCompressWriter::new(file_writer);
/// gzip_writer.write_all(&b"Hello, world"[..])?;
/// # Ok(())
/// # }
/// ```
pub type GzipCompressWriter<W> = crate::io::ProcessorWriter<GzipCompress, W>;

/// `AsyncGzipDecompressReader` is a struct that allows decompression of data using the GZIP format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncReadExt;
/// # use tokio::io::BufReader;
/// # use tokio::fs::File;
/// # use autocompress::gzip::AsyncGzipDecompressReader;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.gz").await?);
/// let mut gzip_reader = AsyncGzipDecompressReader::new(buf_reader);
/// let mut buf = Vec::new();
/// gzip_reader.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncGzipDecompressReader<R> = crate::io::AsyncProcessorReader<GzipDecompress, R>;

/// `AsyncGzipCompressWriter` is a struct that allows compression of data using the GZIP format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncWriteExt;
/// # use tokio::fs::File;
/// # use autocompress::gzip::AsyncGzipCompressWriter;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/doc-gzip-compress-async-writer.gz").await?;
/// let mut gzip_writer = AsyncGzipCompressWriter::new(file_writer);
/// gzip_writer.write_all(&b"Hello, world"[..]).await?;
/// gzip_writer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncGzipCompressWriter<W> = crate::io::AsyncProcessorWriter<GzipCompress, W>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum GzipStatus {
    Header,
    Data,
    Footer,
    StreamEnd,
}

/// `GzipCompress` is a struct that allows compression of data using the GZIP format.
#[derive(Debug)]
pub struct GzipCompress {
    inner: flate2::Compress,
    status: GzipStatus,
    crc: flate2::Crc,
    total_in: u64,
    total_out: u64,
    buffer: Vec<u8>,
}

impl Default for GzipCompress {
    fn default() -> Self {
        Self::new(flate2::Compression::default())
    }
}

impl GzipCompress {
    pub fn new(level: flate2::Compression) -> Self {
        Self {
            inner: flate2::Compress::new(level, false),
            status: GzipStatus::Header,
            crc: flate2::Crc::new(),
            total_in: 0,
            total_out: 0,
            buffer: Vec::new(),
        }
    }
}

impl Processor for GzipCompress {
    fn process(&mut self, input: &[u8], mut output: &mut [u8], flush: Flush) -> Result<Status> {
        loop {
            if self.status == GzipStatus::Header {
                let header = GzipHeader::default();
                header.write(&mut self.buffer)?;
                self.status = GzipStatus::Data;
            }

            if !self.buffer.is_empty() {
                if output.is_empty() {
                    return Ok(Status::Ok);
                } else {
                    let copy_len = output.len().min(self.buffer.len());
                    output[..copy_len].copy_from_slice(&self.buffer[..copy_len]);
                    output = &mut output[copy_len..];
                    self.buffer = self.buffer[copy_len..].to_vec();
                    self.total_out += copy_len as u64;
                }
            }

            match self.status {
                GzipStatus::Header => unreachable!(),
                GzipStatus::Data => {
                    let original_total_in = self.inner.total_in();
                    let original_total_out = self.inner.total_out();
                    let status = self
                        .inner
                        .compress(
                            &input,
                            &mut output,
                            match flush {
                                Flush::Finish => flate2::FlushCompress::Finish,
                                Flush::None => flate2::FlushCompress::None,
                            },
                        )
                        .map_err(|e| Error::CompressError(e.to_string()))?;
                    let current_total_in = self.inner.total_in() - original_total_in;
                    self.crc.update(&input[..current_total_in as usize]);
                    output = &mut output[(self.inner.total_out() - original_total_out) as usize..];

                    self.total_in += self.inner.total_in() - original_total_in;
                    self.total_out += self.inner.total_out() - original_total_out;
                    match status {
                        flate2::Status::Ok => {
                            return Ok(Status::Ok);
                        }
                        flate2::Status::BufError => {
                            return Err(Error::CompressError("BufError".to_string()));
                        }
                        flate2::Status::StreamEnd => {
                            self.status = GzipStatus::Footer;
                        }
                    }
                }
                GzipStatus::Footer => {
                    let footer = GzipFooter {
                        crc32: self.crc.sum(),
                        isize: self.total_in as u32,
                    };
                    footer.write(&mut self.buffer)?;
                    self.status = GzipStatus::StreamEnd;
                }
                GzipStatus::StreamEnd => {
                    return Ok(Status::StreamEnd);
                }
            }
        }
    }

    fn reset(&mut self) {
        self.inner.reset();
        self.status = GzipStatus::Header;
        self.crc.reset();
        self.total_in = 0;
        self.total_out = 0;
    }

    fn total_in(&self) -> u64 {
        self.total_in
    }

    fn total_out(&self) -> u64 {
        self.total_out
    }
}

/// `GzipDecompress` is a struct that allows decompression of data using the GZIP format.
#[derive(Debug)]
pub struct GzipDecompress {
    processed_input: u64,
    header: Option<GzipHeader>,
    buffer: Vec<u8>,
    inner: flate2::Decompress,
    status: GzipStatus,
    crc: flate2::Crc,
}

impl Default for GzipDecompress {
    fn default() -> Self {
        Self::new()
    }
}

impl GzipDecompress {
    pub fn new() -> Self {
        Self {
            processed_input: 0,
            header: None,
            buffer: Vec::new(),
            inner: flate2::Decompress::new(false),
            status: GzipStatus::Header,
            crc: flate2::Crc::new(),
        }
    }
}

const TEMPORARY_COPY_SIZE_UNIT: usize = 100;

impl Processor for GzipDecompress {
    fn process(&mut self, mut input: &[u8], output: &mut [u8], flush: Flush) -> Result<Status> {
        loop {
            match self.status {
                GzipStatus::Header => {
                    //eprintln!("parse header loop: {}", self.buffer.len());
                    let copied_size = TEMPORARY_COPY_SIZE_UNIT.min(input.len());
                    if copied_size == 0 {
                        return Ok(Status::Ok);
                    }
                    let original_buffer_size = self.buffer.len();
                    self.buffer.extend_from_slice(&input[..copied_size]);
                    match GzipHeader::parse(&self.buffer[..]) {
                        Ok(header) => {
                            let consumed_size = header.header_size - original_buffer_size;
                            self.header = Some(header);
                            input = &input[consumed_size..];
                            self.processed_input += consumed_size as u64;
                            self.status = GzipStatus::Data;
                            self.buffer.clear();
                            //eprintln!("Gzip header parsed: {:?}", self.header);
                        }
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                                input = &input[copied_size..];
                                self.processed_input += copied_size as u64;
                            } else {
                                return Err(e.into());
                            }
                        }
                    }
                }
                GzipStatus::Data => {
                    if input.is_empty() {
                        return match flush {
                            Flush::Finish => Err(Error::MoreDataRequired),
                            Flush::None => Ok(Status::Ok),
                        };
                    }
                    // eprintln!("decompression loop");
                    let original_total_in = self.inner.total_in();
                    let original_total_out = self.inner.total_out();
                    let status = self
                        .inner
                        .decompress(input, output, flate2::FlushDecompress::None)
                        .map_err(|e| Error::DecompressError(e.to_string()))?;
                    let current_processed_input = self.inner.total_in() - original_total_in;
                    let current_processed_output = self.inner.total_out() - original_total_out;
                    self.crc
                        .update(&output[..current_processed_output as usize]);
                    self.processed_input += current_processed_input;
                    input = &input[current_processed_input as usize..];
                    match status {
                        flate2::Status::Ok => {
                            return match flush {
                                Flush::Finish => Err(Error::MoreDataRequired),
                                Flush::None => Ok(Status::Ok),
                            }
                        }
                        flate2::Status::BufError => {
                            return Err(Error::DecompressError("BufError".to_string()))
                        }
                        flate2::Status::StreamEnd => {
                            self.status = GzipStatus::Footer;
                        }
                    }
                }
                GzipStatus::Footer => {
                    //eprintln!("parse footer loop");
                    let copied_size = TEMPORARY_COPY_SIZE_UNIT.min(input.len());
                    if copied_size == 0 {
                        return match flush {
                            Flush::Finish => Err(Error::MoreDataRequired),
                            Flush::None => Ok(Status::Ok),
                        };
                    }
                    let original_buffer_size = self.buffer.len();
                    self.buffer.extend_from_slice(&input[..copied_size]);
                    match GzipFooter::parse(&self.buffer[..]) {
                        Ok(footer) => {
                            let consumed_size = footer.footer_size() - original_buffer_size;
                            self.processed_input += consumed_size as u64;
                            if footer.crc32 != self.crc.sum() {
                                return Err(Error::DecompressError("CRC32 mismatch".to_string()));
                            }
                            self.status = GzipStatus::StreamEnd;
                            self.buffer.clear();

                            self.crc.reset();
                            //eprintln!("Gzip footer parsed: {:?}", footer);
                            return Ok(Status::StreamEnd);
                        }
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                                input = &input[copied_size..];
                                self.processed_input += copied_size as u64;
                            } else {
                                return Err(e.into());
                            }
                        }
                    }
                }
                GzipStatus::StreamEnd => {
                    return Ok(Status::StreamEnd);
                }
            }
        }
    }

    fn reset(&mut self) {
        self.processed_input = 0;
        self.header = None;
        self.buffer.clear();
        self.inner.reset(false);
        self.status = GzipStatus::Header;
        self.crc.reset();
    }

    fn total_in(&self) -> u64 {
        self.processed_input
    }
    fn total_out(&self) -> u64 {
        self.inner.total_out()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::test_decompress;

    #[test]
    fn test_gzip_decompress() -> anyhow::Result<()> {
        let data = include_bytes!("../../testfiles/sqlite3.c.gz");
        let decompress = GzipDecompress::new();
        test_decompress(decompress, &data[..])?;

        Ok(())
    }

    #[test]
    fn test_gzip_pipe() -> crate::error::Result<()> {
        let data = include_bytes!("../../testfiles/sqlite3.c.pipe.gz");
        test_decompress(GzipDecompress::new(), data)?;
        Ok(())
    }

    #[test]
    fn test_gzip_pigz() -> crate::error::Result<()> {
        let data = include_bytes!("../../testfiles/sqlite3.c.pigz.gz");
        test_decompress(GzipDecompress::new(), data)?;
        Ok(())
    }

    #[test]
    fn test_gzip_multistream() -> crate::error::Result<()> {
        let data = include_bytes!("../../testfiles/sqlite3.c.multistream.gz");
        test_decompress(GzipDecompress::new(), data)?;
        Ok(())
    }

    #[test]
    fn test_gzip_bgzip() -> crate::error::Result<()> {
        let data = include_bytes!("../../testfiles/sqlite3.c.bgzip.gz");
        test_decompress(GzipDecompress::new(), data)?;
        Ok(())
    }

    #[test]
    fn test_gzip_compress() -> anyhow::Result<()> {
        crate::tests::test_compress(
            || Ok(GzipCompress::new(flate2::Compression::default())),
            || Ok(GzipDecompress::new()),
        )?;

        Ok(())
    }
}
