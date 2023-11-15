#[cfg(feature = "tokio")]
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite};

#[cfg(feature = "bgzip")]
use crate::bgzip::BgzipCompress;
#[cfg(feature = "bzip2")]
use crate::bzip2::{Bzip2Compress, Bzip2Decompress};
#[cfg(feature = "flate2")]
use crate::gzip::{GzipCompress, GzipDecompress};
#[cfg(feature = "xz")]
use crate::xz::{XzCompress, XzDecompress};
#[cfg(feature = "zstd")]
use crate::zstd::{ZstdCompress, ZstdDecompress};

#[cfg(feature = "tokio")]
use crate::{io::AsyncProcessorReader, io::AsyncProcessorWriter};
use crate::{
    io::ProcessorReader, io::ProcessorWriter, CompressionLevel, PlainProcessor, Processor,
};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Result, Write},
    path::Path,
};

/// File Format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    #[cfg(feature = "flate2")]
    /// Gzip format
    ///
    /// Expected file extension is `.gz`
    Gzip,
    #[cfg(feature = "bgzip")]
    /// BGZF format
    ///
    /// Specification of this format is available at [htslib webpage](https://samtools.github.io/hts-specs/SAMv1.pdf).
    /// Expected file extension is `.bgz`
    BGZip,
    #[cfg(feature = "bzip2")]
    /// Bzip2 format
    ///
    /// Expected file extension is `.bz2`
    Bzip2,
    #[cfg(feature = "xz")]
    /// XZ format
    ///
    /// Expected file extension is `.xz`
    Xz,
    #[cfg(feature = "zstd")]
    /// Zstandard format
    ///
    /// Expected file extension is `.zst`
    Zstd,
}

impl FileFormat {
    pub fn extension(self) -> &'static str {
        match self {
            #[cfg(feature = "flate2")]
            Self::Gzip => "gz",
            #[cfg(feature = "bgzip")]
            Self::BGZip => "bgz",
            #[cfg(feature = "bzip2")]
            Self::Bzip2 => "bz2",
            #[cfg(feature = "xz")]
            Self::Xz => "xz",
            #[cfg(feature = "zstd")]
            Self::Zstd => "zst",
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Option<Self> {
        let ext = path.as_ref().extension().and_then(|s| s.to_str());
        match ext {
            #[cfg(feature = "flate2")]
            Some("gz") => Some(Self::Gzip),
            #[cfg(feature = "bgzip")]
            Some("bgz") => Some(Self::BGZip),
            #[cfg(feature = "bzip2")]
            Some("bz2") => Some(Self::Bzip2),
            #[cfg(feature = "xz")]
            Some("xz") => Some(Self::Xz),
            #[cfg(feature = "zstd")]
            Some("zst") => Some(Self::Zstd),
            _ => None,
        }
    }

    pub fn from_buf(buf: &[u8]) -> Option<Self> {
        if buf.starts_with(&[0x1f, 0x8b]) {
            #[cfg(feature = "flate2")]
            return Some(Self::Gzip);
        }
        if buf.starts_with(b"BZ") {
            #[cfg(feature = "bzip2")]
            return Some(Self::Bzip2);
        }
        if buf.starts_with(&[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00]) {
            #[cfg(feature = "xz")]
            return Some(Self::Xz);
        }
        if buf.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
            #[cfg(feature = "zstd")]
            return Some(Self::Zstd);
        }
        None
    }

    pub fn from_buf_reader<R: BufRead>(mut reader: R) -> Result<Option<Self>> {
        let signature = reader.fill_buf()?;
        Ok(Self::from_buf(signature))
    }

    #[cfg(feature = "tokio")]
    pub async fn from_async_buf_reader<R: AsyncBufRead + Unpin>(
        mut reader: R,
    ) -> Result<Option<Self>> {
        let signature = reader.fill_buf().await?;
        Ok(Self::from_buf(signature))
    }

    pub fn compressor(
        self,
        compression_level: CompressionLevel,
    ) -> Box<dyn Processor + Unpin + Send> {
        match self {
            #[cfg(feature = "flate2")]
            Self::Gzip => Box::new(GzipCompress::new(compression_level.flate2())),
            #[cfg(feature = "bgzip")]
            Self::BGZip => Box::new(BgzipCompress::new(compression_level.bgzip())),
            #[cfg(feature = "bzip2")]
            Self::Bzip2 => Box::new(Bzip2Compress::new(compression_level.bzip2())),
            #[cfg(feature = "xz")]
            Self::Xz => Box::new(XzCompress::new(compression_level.xz()).unwrap()),
            #[cfg(feature = "zstd")]
            Self::Zstd => Box::new(ZstdCompress::new(compression_level.zstd()).unwrap()),
        }
    }

    pub fn decompressor(self) -> Box<dyn Processor + Unpin + Send> {
        match self {
            #[cfg(feature = "flate2")]
            Self::Gzip => Box::new(GzipDecompress::new()),
            #[cfg(feature = "bgzip")]
            Self::BGZip => Box::new(GzipDecompress::new()),
            #[cfg(feature = "bzip2")]
            Self::Bzip2 => Box::new(Bzip2Decompress::new()),
            #[cfg(feature = "xz")]
            Self::Xz => Box::new(XzDecompress::new(10_000_000).unwrap()),
            #[cfg(feature = "zstd")]
            Self::Zstd => Box::new(ZstdDecompress::new().unwrap()),
        }
    }
}

/// Automatically select suitable decoder from magic number from [`Read`].
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::io::BufReader;
/// # use std::fs::File;
/// use autocompress::autodetect_reader;
///
/// # fn main() -> anyhow::Result<()> {
/// let file_reader = File::open("testfiles/sqlite3.c.zst")?;
/// let mut reader = autodetect_reader(file_reader)?;
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub fn autodetect_reader<R: Read>(
    reader: R,
) -> Result<ProcessorReader<Box<dyn Processor + Send + Unpin>, std::io::BufReader<R>>> {
    autodetect_buf_reader(BufReader::new(reader))
}

/// Automatically select suitable decoder from magic number from [`BufRead`].
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::io::BufReader;
/// # use std::fs::File;
/// use autocompress::autodetect_buf_reader;
///
/// # fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.zst")?);
/// let mut reader = autodetect_buf_reader(buf_reader)?;
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub fn autodetect_buf_reader<R: BufRead>(
    mut reader: R,
) -> Result<ProcessorReader<Box<dyn Processor + Send + Unpin>, R>> {
    let decompressor = if let Some(format) = FileFormat::from_buf_reader(&mut reader)? {
        format.decompressor()
    } else {
        Box::new(PlainProcessor::new())
    };
    Ok(ProcessorReader::with_processor(decompressor, reader))
}

/// Open a file and automatically select a suitable decoder from magic number.
///
/// File extension is not effect to decoder selection.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// use autocompress::autodetect_open;
///
/// # fn main() -> anyhow::Result<()> {
/// let mut reader = autodetect_open("testfiles/sqlite3.c.zst")?;
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub fn autodetect_open<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<ProcessorReader<Box<dyn Processor + Send + Unpin>, std::io::BufReader<std::fs::File>>> {
    let file = std::fs::File::open(path)?;
    autodetect_buf_reader(std::io::BufReader::new(file))
}

/// Open a file or standard input, and automatically select suitable decoder from magic number.
///
/// File extension is not effect to decoder selection.
/// If `path` is `None`, standard input is used.
///
///  ## Example
/// ```
/// # use std::io::prelude::*;
/// use autocompress::autodetect_open_or_stdin;
///
/// # fn main() -> anyhow::Result<()> {
/// let mut reader = autodetect_open_or_stdin(Some("testfiles/sqlite3.c.zst"))?;
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub fn autodetect_open_or_stdin<P: AsRef<Path>>(
    path: Option<P>,
) -> Result<impl Read + Send + Unpin> {
    let file: Box<dyn BufRead + Send + Unpin> = if let Some(path) = path.as_ref() {
        Box::new(std::io::BufReader::new(std::fs::File::open(path)?))
    } else {
        Box::new(std::io::BufReader::new(std::io::stdin()))
    };
    autodetect_buf_reader(file)
}

#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
/// Automatically select suitable decoder from magic number from [`AsyncBufRead`].
pub async fn autodetect_async_buf_reader<R: AsyncBufRead + Unpin>(
    mut reader: R,
) -> Result<AsyncProcessorReader<Box<dyn Processor + Send + Unpin>, R>> {
    let decompressor = if let Some(format) = FileFormat::from_async_buf_reader(&mut reader).await? {
        format.decompressor()
    } else {
        Box::new(PlainProcessor::new())
    };
    Ok(AsyncProcessorReader::with_processor(decompressor, reader))
}

#[cfg(feature = "tokio_fs")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio_fs")))]
/// Open a file and automatically select a suitable decoder from magic number.
pub async fn autodetect_async_open<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<
    AsyncProcessorReader<Box<dyn Processor + Send + Unpin>, tokio::io::BufReader<tokio::fs::File>>,
> {
    let file = tokio::fs::File::open(path).await?;
    autodetect_async_buf_reader(tokio::io::BufReader::new(file)).await
}

#[cfg(feature = "tokio_fs")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio_fs")))]
/// Open a file or standard input, and automatically select suitable decoder from magic number.
///
/// If `path` is `None`, standard input is used.
pub async fn autodetect_async_open_or_stdin<P: AsRef<Path>>(
    path: Option<P>,
) -> Result<impl AsyncRead> {
    use std::pin::Pin;

    let file: Pin<Box<dyn AsyncBufRead>> = if let Some(path) = path.as_ref() {
        Box::pin(tokio::io::BufReader::new(
            tokio::fs::File::open(path).await?,
        ))
    } else {
        Box::pin(tokio::io::BufReader::new(tokio::io::stdin()))
    };
    autodetect_async_buf_reader(tokio::io::BufReader::new(file)).await
}

/// Create a file and automatically select a suitable encoder from file extension.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// use autocompress::{autodetect_create, CompressionLevel};
///
/// # fn main() -> anyhow::Result<()> {
/// let mut writer = autodetect_create("target/doc-autodetect_create.zst", CompressionLevel::Default)?;
/// writer.write_all(&b"Hello, world\n"[..])?;
/// # Ok(())
/// # }
/// ```
pub fn autodetect_create<P: AsRef<Path>>(
    path: P,
    compression_level: CompressionLevel,
) -> Result<ProcessorWriter<Box<dyn Processor + Send + Unpin>, File>> {
    let compressor = if let Some(format) = FileFormat::from_path(path.as_ref()) {
        format.compressor(compression_level)
    } else {
        Box::new(PlainProcessor::new())
    };
    Ok(ProcessorWriter::with_processor(
        compressor,
        File::create(path)?,
    ))
}

#[cfg(feature = "bgzip")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "bgzip")))]
/// Create a file and automatically select a suitable encoder from file extension.
///
/// This function prefers BGZip format to gzip formats. If file extension is `.gz`, BGZip format is used.
pub fn autodetect_create_prefer_bgzip<P: AsRef<Path>>(
    path: P,
    compression_level: CompressionLevel,
) -> Result<ProcessorWriter<Box<dyn Processor + Send + Unpin>, File>> {
    let compressor = if let Some(format) = FileFormat::from_path(path.as_ref()) {
        match format {
            FileFormat::Gzip => Box::new(BgzipCompress::new(compression_level.bgzip())),
            _ => format.compressor(compression_level),
        }
    } else {
        Box::new(BgzipCompress::new(compression_level.bgzip()))
    };
    Ok(ProcessorWriter::with_processor(
        compressor,
        File::create(path)?,
    ))
}

/// Create a file or open standard input, and automatically select a suitable encoder from file extension.
pub fn autodetect_create_or_stdout<P: AsRef<Path>>(
    path: Option<P>,
    compression_level: CompressionLevel,
) -> Result<ProcessorWriter<Box<dyn Processor + Send + Unpin>, Box<dyn Write + Send + Unpin>>> {
    let compressor = if let Some(path) = path.as_ref() {
        if let Some(format) = FileFormat::from_path(path.as_ref()) {
            format.compressor(compression_level)
        } else {
            Box::new(PlainProcessor::new())
        }
    } else {
        Box::new(PlainProcessor::new())
    };
    let output: Box<dyn Write + Send + Unpin> = if let Some(path) = path {
        Box::new(File::create(path)?)
    } else {
        Box::new(std::io::stdout())
    };
    Ok(ProcessorWriter::with_processor(compressor, output))
}

#[cfg(feature = "bgzip")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "bgzip")))]
/// Create a file or open standard input, and automatically select a suitable encoder from file extension.
///
/// This function prefers BGZip format to gzip formats. If file extension is `.gz`, BGZip format is used.
/// When `path` is `None` and standard output is a terminal, plain format is used.
/// When `path` is `None` and standard output is not a terminal, BGzip format is used.
pub fn autodetect_create_or_stdout_prefer_bgzip<P: AsRef<Path>>(
    path: Option<P>,
    compression_level: CompressionLevel,
) -> Result<ProcessorWriter<Box<dyn Processor + Send + Unpin>, Box<dyn Write + Send + Unpin>>> {
    use std::io::IsTerminal;

    let compressor: Box<dyn Processor + Send + Unpin> = if let Some(path) = path.as_ref() {
        if let Some(format) = FileFormat::from_path(path.as_ref()) {
            match format {
                FileFormat::Gzip => Box::new(BgzipCompress::new(compression_level.bgzip())),
                _ => format.compressor(compression_level),
            }
        } else {
            Box::new(BgzipCompress::new(compression_level.bgzip()))
        }
    } else {
        let c: Box<dyn Processor + Send + Unpin> = if !std::io::stdout().is_terminal() {
            Box::new(BgzipCompress::new(compression_level.bgzip()))
        } else {
            Box::new(PlainProcessor::new())
        };
        c
    };
    let output: Box<dyn Write + Send + Unpin> = if let Some(path) = path {
        Box::new(File::create(path)?)
    } else {
        Box::new(std::io::stdout())
    };
    Ok(ProcessorWriter::with_processor(compressor, output))
}

#[cfg(feature = "tokio_fs")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio_fs")))]
/// Create a file, and automatically select a suitable encoder from file extension, async version.
pub async fn autodetect_async_create<P: AsRef<Path>>(
    path: P,
    compression_level: CompressionLevel,
) -> Result<AsyncProcessorWriter<Box<dyn Processor + Send + Unpin>, tokio::fs::File>> {
    let compressor = if let Some(format) = FileFormat::from_path(path.as_ref()) {
        format.compressor(compression_level)
    } else {
        Box::new(PlainProcessor::new())
    };
    Ok(AsyncProcessorWriter::with_processor(
        compressor,
        tokio::fs::File::create(path).await?,
    ))
}

#[cfg(feature = "tokio_fs")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio_fs")))]
/// Create a file or open standard input, and automatically select a suitable encoder from file extension, async version.
pub async fn autodetect_async_create_or_stdout<P: AsRef<Path>>(
    path: Option<P>,
    compression_level: CompressionLevel,
) -> Result<
    AsyncProcessorWriter<Box<dyn Processor + Send + Unpin>, std::pin::Pin<Box<dyn AsyncWrite>>>,
> {
    let compressor = if let Some(path) = path.as_ref() {
        if let Some(format) = FileFormat::from_path(path.as_ref()) {
            format.compressor(compression_level)
        } else {
            Box::new(PlainProcessor::new())
        }
    } else {
        Box::new(PlainProcessor::new())
    };
    let output: std::pin::Pin<Box<dyn AsyncWrite>> = if let Some(path) = path {
        Box::pin(tokio::fs::File::create(path).await?)
    } else {
        Box::pin(tokio::io::stdout())
    };

    Ok(AsyncProcessorWriter::with_processor(compressor, output))
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Context;
    use std::fs::File;
    use std::io::BufReader;
    #[cfg(feature = "tokio")]
    use tokio::io::AsyncReadExt;

    const FILE_LIST: &[&'static str] = &[
        "testfiles/sqlite3.c",
        #[cfg(feature = "flate2")]
        "testfiles/sqlite3.c.bgzip.gz",
        #[cfg(feature = "bzip2")]
        "testfiles/sqlite3.c.bz2",
        #[cfg(feature = "flate2")]
        "testfiles/sqlite3.c.gz",
        #[cfg(feature = "bzip2")]
        "testfiles/sqlite3.c.multistream.bz2",
        #[cfg(feature = "flate2")]
        "testfiles/sqlite3.c.multistream.gz",
        #[cfg(feature = "xz")]
        "testfiles/sqlite3.c.multistream.xz",
        #[cfg(feature = "zstd")]
        "testfiles/sqlite3.c.multistream.zst",
        #[cfg(feature = "flate2")]
        "testfiles/sqlite3.c.pigz.gz",
        #[cfg(feature = "flate2")]
        "testfiles/sqlite3.c.pipe.gz",
        #[cfg(feature = "xz")]
        "testfiles/sqlite3.c.xz",
        #[cfg(feature = "zstd")]
        "testfiles/sqlite3.c.zst",
    ];

    #[test]
    fn test_read() -> anyhow::Result<()> {
        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_file in FILE_LIST {
            let mut read_data = Vec::new();
            autodetect_reader(BufReader::new(File::open(one_file)?))?
                .read_to_end(&mut read_data)?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_read_rayon() -> anyhow::Result<()> {
        use crate::io::RayonReader;

        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_file in FILE_LIST {
            let mut read_data = Vec::new();
            RayonReader::new(autodetect_reader(BufReader::new(File::open(one_file)?))?)
                .read_to_end(&mut read_data)?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_read_async() -> anyhow::Result<()> {
        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_file in FILE_LIST {
            let mut read_data = Vec::new();
            autodetect_async_buf_reader(tokio::io::BufReader::new(
                tokio::fs::File::open(one_file).await?,
            ))
            .await?
            .read_to_end(&mut read_data)
            .await?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[test]
    fn test_file() -> anyhow::Result<()> {
        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_file in FILE_LIST {
            let mut read_data = Vec::new();
            autodetect_open(one_file)?.read_to_end(&mut read_data)?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);

            let mut read_data = Vec::new();
            autodetect_open_or_stdin(Some(one_file))?.read_to_end(&mut read_data)?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_file_rayon() -> anyhow::Result<()> {
        use crate::io::RayonReader;

        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_file in FILE_LIST {
            let mut read_data = Vec::new();
            RayonReader::new(autodetect_open(one_file)?).read_to_end(&mut read_data)?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);

            let mut read_data = Vec::new();
            RayonReader::new(autodetect_open_or_stdin(Some(one_file))?)
                .read_to_end(&mut read_data)?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[test]
    fn test_file_write() -> anyhow::Result<()> {
        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_format in &[
            #[cfg(feature = "gzip")]
            FileFormat::Gzip,
            #[cfg(feature = "bgzip")]
            FileFormat::BGZip,
            #[cfg(feature = "bzip2")]
            FileFormat::Bzip2,
            #[cfg(feature = "xz")]
            FileFormat::Xz,
            #[cfg(feature = "zstd")]
            FileFormat::Zstd,
        ] {
            let output_filename =
                format!("target/test_autodetect_write.{}", one_format.extension());
            let mut writer = autodetect_create(&output_filename, CompressionLevel::Default)?;
            writer
                .write_all(&expected_data)
                .with_context(|| format!("write_all error: {:?}", one_format))?;
            writer
                .flush()
                .with_context(|| format!("flush error: {:?}", one_format))?;
            let inner_writer = writer.into_inner_writer();
            inner_writer.sync_all()?;

            let mut read_data = Vec::new();
            let mut reader = ProcessorReader::with_processor(
                one_format.decompressor(),
                std::io::BufReader::new(
                    File::open(&output_filename)
                        .with_context(|| format!("file open error: {:?}", one_format))?,
                ),
            );
            reader
                .read_to_end(&mut read_data)
                .with_context(|| format!("read_to_end error: {:?}", one_format))?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[cfg(feature = "tokio_fs")]
    #[tokio::test]
    async fn test_file_write_async() -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut expected_data = Vec::new();
        tokio::fs::File::open("testfiles/sqlite3.c")
            .await?
            .read_to_end(&mut expected_data)
            .await?;

        for one_format in &[
            #[cfg(feature = "gzip")]
            FileFormat::Gzip,
            #[cfg(feature = "bgzip")]
            FileFormat::BGZip,
            #[cfg(feature = "bzip2")]
            FileFormat::Bzip2,
            #[cfg(feature = "xz")]
            FileFormat::Xz,
            #[cfg(feature = "zstd")]
            FileFormat::Zstd,
        ] {
            let output_filename = format!(
                "target/test_autodetect_async_write.{}",
                one_format.extension()
            );
            let mut writer =
                autodetect_async_create(&output_filename, CompressionLevel::Default).await?;
            writer
                .write_all(&expected_data)
                .await
                .with_context(|| format!("write_all: {:?}", one_format))?;
            writer
                .flush()
                .await
                .with_context(|| format!("flush error: {:?}", one_format))?;

            let mut read_data = Vec::new();
            let mut reader = AsyncProcessorReader::with_processor(
                one_format.decompressor(),
                tokio::io::BufReader::new(
                    tokio::fs::File::open(&output_filename)
                        .await
                        .with_context(|| format!("file open error: {:?}", one_format))?,
                ),
            );
            reader
                .read_to_end(&mut read_data)
                .await
                .with_context(|| format!("read_to_end error: {:?}", one_format))?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn test_file_write_rayon() -> anyhow::Result<()> {
        use anyhow::Context;

        use crate::io::{RayonReader, RayonWriter};

        let mut expected_data = Vec::new();
        File::open("testfiles/sqlite3.c")?.read_to_end(&mut expected_data)?;

        for one_format in &[
            #[cfg(feature = "gzip")]
            FileFormat::Gzip,
            #[cfg(feature = "bgzip")]
            FileFormat::BGZip,
            #[cfg(feature = "bzip2")]
            FileFormat::Bzip2,
            #[cfg(feature = "xz")]
            FileFormat::Xz,
            #[cfg(feature = "zstd")]
            FileFormat::Zstd,
        ] {
            let output_filename = format!(
                "target/test_autodetect_rayon_write.{}",
                one_format.extension()
            );
            let mut writer = RayonWriter::new(autodetect_create(
                output_filename.clone(),
                CompressionLevel::Default,
            )?);
            writer
                .write_all(&expected_data)
                .with_context(|| format!("write_all error: {:?}", one_format))?;
            writer
                .flush()
                .with_context(|| format!("flush error: {:?}", one_format))?;
            std::mem::drop(writer);
            std::thread::sleep(std::time::Duration::from_millis(100));

            let mut read_data = Vec::new();
            let mut reader = RayonReader::new(ProcessorReader::with_processor(
                one_format.decompressor(),
                std::io::BufReader::new(File::open(&output_filename)?),
            ));
            reader
                .read_to_end(&mut read_data)
                .with_context(|| format!("read_to_end error: {:?}", one_format))?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[cfg(feature = "tokio_fs")]
    #[tokio::test]
    async fn test_file_async() -> anyhow::Result<()> {
        let expected_data = include_bytes!("../testfiles/sqlite3.c");

        for one_file in FILE_LIST {
            let mut read_data = Vec::new();
            autodetect_async_open(one_file)
                .await?
                .read_to_end(&mut read_data)
                .await?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);

            let mut read_data = Vec::new();
            autodetect_async_open_or_stdin(Some(one_file))
                .await?
                .read_to_end(&mut read_data)
                .await?;
            assert_eq!(read_data.len(), expected_data.len());
            assert_eq!(read_data, expected_data);
        }

        Ok(())
    }

    #[test]
    fn test_file_format_from_path() {
        #[cfg(feature = "flate2")]
        assert_eq!(FileFormat::from_path("test.gz"), Some(FileFormat::Gzip));
        #[cfg(feature = "bgzip")]
        assert_eq!(FileFormat::from_path("test.bgz"), Some(FileFormat::BGZip));
        #[cfg(feature = "xz")]
        assert_eq!(FileFormat::from_path("test.xz"), Some(FileFormat::Xz));
        #[cfg(feature = "bzip2")]
        assert_eq!(FileFormat::from_path("test.bz2"), Some(FileFormat::Bzip2));
        #[cfg(feature = "zstd")]
        assert_eq!(FileFormat::from_path("test.zst"), Some(FileFormat::Zstd));
        assert_eq!(FileFormat::from_path("test.c"), None);
        assert_eq!(FileFormat::from_path("test"), None);
    }
}
