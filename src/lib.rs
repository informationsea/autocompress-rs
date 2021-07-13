//! Automatically select suitable decoder from magic bytes or encoder from file extension.
//! This library also provides I/O thread pool to perform decompression and compression in background threads.
//!
//! Supported file formats
//! ---------------------
//!
//! * [Gzip](https://www.ietf.org/rfc/rfc1952.txt)
//! * [Zlib](https://www.ietf.org/rfc/rfc1950.txt) (Cannot suggest format from magic bytes and file extension)
//! * [BZip2](https://www.sourceware.org/bzip2/)
//! * [XZ](https://tukaani.org/xz/format.html)
//! * [Snappy](https://github.com/google/snappy) (Cannot suggest format from file extension)
//! * [Z-standard](https://facebook.github.io/zstd/)
//! * [LZ4](https://www.lz4.org/)
//! * [Brotli](https://github.com/google/brotli) (Cannot suggest format from magic bytes)
//!
//! ## Example
//! ```
//! use autocompress::open;
//! use std::io::{self, Read};
//!
//! fn main() -> io::Result<()> {
//! # #[cfg(feature = "xz2")] {
//!   let mut buffer = Vec::new();
//!   open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
//!   assert_eq!(buffer, b"ABCDEFG\r\n1234567");
//! # }
//!   Ok(())
//! }
//! ```
//!
//! ## I/O thread example
//! ```
//! # #[cfg(feature = "thread")] {
//! use autocompress::{iothread::IoThread, open, create, CompressionLevel};
//!
//! # use std::io::{prelude::*, self};
//! # fn main() -> io::Result<()> {
//! let thread_pool = IoThread::new(2);
//! let mut threaded_reader = thread_pool.add_reader(open("testfiles/plain.txt.xz")?)?;
//! let mut threaded_writer = thread_pool.add_writer(create("target/plain.txt.xz", CompressionLevel::Default)?);
//! let mut buffer = Vec::new();
//! threaded_reader.read_to_end(&mut buffer)?;
//! assert_eq!(buffer, b"ABCDEFG\r\n1234567");
//! threaded_writer.write_all(b"ABCDEFG\r\n1234567")?;
//! # Ok(())
//! # }
//! # }
//! ```

mod read;
mod write;

#[cfg(feature = "thread")]
pub mod iothread;

pub use read::Decoder;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, prelude::*};
use std::path;
pub use write::{CompressionLevel, Encoder};

/// List of supported file formats
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Format {
    #[cfg(feature = "flate2")]
    Gzip,
    #[cfg(feature = "flate2")]
    Zlib,
    #[cfg(feature = "bzip2")]
    Bzip2,
    #[cfg(feature = "xz2")]
    Xz,
    #[cfg(feature = "snap")]
    Snappy,
    #[cfg(feature = "zstd")]
    Zstd,
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "brotli")]
    Brotli,
    Unknown,
}

/// Suggest file format from magic bytes.
/// This function does not consume data.
pub fn suggest_format(mut reader: impl io::BufRead) -> io::Result<Format> {
    let buffer = reader.fill_buf()?;
    #[cfg(feature = "flate2")]
    if buffer.starts_with(&[0x1f, 0x8b]) {
        return Ok(Format::Gzip);
    }
    #[cfg(feature = "bzip2")]
    if buffer.starts_with(b"BZ") {
        return Ok(Format::Bzip2);
    }
    #[cfg(feature = "snap")]
    if buffer.starts_with(b"sNaPpY") {
        return Ok(Format::Snappy);
    }
    #[cfg(feature = "xz2")]
    if buffer.starts_with(&[0xFD, b'7', b'z', b'X', b'Z', 0x00]) {
        return Ok(Format::Xz);
    }
    #[cfg(feature = "zstd")]
    if buffer.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return Ok(Format::Zstd);
    }
    #[cfg(feature = "lz4")]
    if buffer.starts_with(&[0x04, 0x22, 0x4D, 0x18]) {
        return Ok(Format::Lz4);
    }
    Ok(Format::Unknown)
}

/// Suggest file format from a path extension
pub fn suggest_format_from_path(path: impl AsRef<path::Path>) -> Format {
    #[cfg(feature = "brotli")]
    if path.as_ref().extension() == Some(OsStr::new("br")) {
        return Format::Brotli;
    }
    #[cfg(feature = "flate2")]
    if path.as_ref().extension() == Some(OsStr::new("gz")) {
        return Format::Gzip;
    }
    #[cfg(feature = "bzip2")]
    if path.as_ref().extension() == Some(OsStr::new("bz2")) {
        return Format::Bzip2;
    }
    #[cfg(feature = "xz2")]
    if path.as_ref().extension() == Some(OsStr::new("xz")) {
        return Format::Xz;
    }
    #[cfg(feature = "zstd")]
    if path.as_ref().extension() == Some(OsStr::new("zst")) {
        return Format::Zstd;
    }
    #[cfg(feature = "lz4")]
    if path.as_ref().extension() == Some(OsStr::new("lz4")) {
        return Format::Lz4;
    }
    Format::Unknown
}

/// Open new reader from file path. File format is suggested from magic bytes and file extension.
///
/// ```
/// # use autocompress::*;
/// # use std::io::{self, Read};
/// #
/// # fn main() -> io::Result<()> {
/// # #[cfg(feature = "xz2")] {
/// let mut buffer = Vec::new();
/// open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
/// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
/// # }
/// #  Ok(())
/// # }
/// ```
pub fn open(path: impl AsRef<path::Path>) -> io::Result<read::Decoder<io::BufReader<fs::File>>> {
    let mut reader = io::BufReader::new(fs::File::open(&path)?);
    let mut format = suggest_format(&mut reader)?;
    #[cfg(feature = "brotli")]
    if format == Format::Unknown && path.as_ref().extension() == Some(OsStr::new("br")) {
        format = Format::Brotli;
    }
    Decoder::new(reader, format)
}

/// Open new reader from file path. If path is `None`, standard input is used. File format is suggested from magic bytes and file extension.
///
/// ```
/// # use autocompress::*;
/// # use std::io::{self, Read};
/// #
/// # fn main() -> io::Result<()> {
/// # #[cfg(feature = "xz2")] {
/// let mut buffer = Vec::new();
/// open_or_stdin(Some("testfiles/plain.txt.xz"))?.read_to_end(&mut buffer)?;
/// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
/// # }
/// #  Ok(())
/// # }
/// ```
pub fn open_or_stdin(
    path: Option<impl AsRef<path::Path>>,
) -> io::Result<read::Decoder<io::BufReader<Box<dyn io::Read>>>> {
    if let Some(path) = path {
        let mut reader: io::BufReader<Box<dyn io::Read>> =
            io::BufReader::new(Box::new(fs::File::open(&path)?));
        let mut format = suggest_format(&mut reader)?;
        #[cfg(feature = "brotli")]
        if format == Format::Unknown && path.as_ref().extension() == Some(OsStr::new("br")) {
            format = Format::Brotli;
        }
        Decoder::new(reader, format)
    } else {
        let mut reader: io::BufReader<Box<dyn io::Read>> =
            io::BufReader::new(Box::new(io::stdin()));
        let format = suggest_format(&mut reader)?;
        Decoder::new(reader, format)
    }
}

/// Create new writer from file path. File format is suggested from file extension.
///
/// ```
/// # use autocompress::*;
/// # use std::io::{self, Write};
/// #
/// # fn main() -> io::Result<()> {
/// let mut writer = create("create.txt.lz4", CompressionLevel::Default)?;
/// writer.write_all(b"hello, world")?;
/// # std::fs::remove_file("create.txt.lz4")?;
/// #  Ok(())
/// # }
/// ```
pub fn create(
    path: impl AsRef<path::Path>,
    level: CompressionLevel,
) -> io::Result<write::Encoder<fs::File>> {
    let format = suggest_format_from_path(&path);
    Encoder::new(fs::File::create(path)?, format, level)
}

/// Create new writer from a file path. If a file path is `None`, standard output is used. File format is suggested from file extension.
///
/// ```
/// # use autocompress::*;
/// # use std::io::{self, Write};
/// #
/// # fn main() -> io::Result<()> {
/// # #[cfg(feature = "lz4")] {
/// let mut writer = create_or_stdout(Some("create.txt.lz4"), CompressionLevel::Default)?;
/// writer.write_all(b"hello, world")?;
/// # std::fs::remove_file("create.txt.lz4")?;
/// # }
/// #  Ok(())
/// # }
/// ```
pub fn create_or_stdout(
    path: Option<impl AsRef<path::Path>>,
    level: CompressionLevel,
) -> io::Result<Box<dyn Write + Send>> {
    if let Some(path) = path {
        Ok(Box::new(create(path, level)?))
    } else {
        Ok(Box::new(io::stdout()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_suggest_format() {
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt")[..]).unwrap(),
            Format::Unknown
        );
        #[cfg(feature = "flate2")]
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.gz")[..]).unwrap(),
            Format::Gzip
        );
        #[cfg(feature = "bzip2")]
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.bz2")[..]).unwrap(),
            Format::Bzip2
        );
        #[cfg(feature = "xz2")]
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.xz")[..]).unwrap(),
            Format::Xz
        );
        #[cfg(feature = "zstd")]
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.zst")[..]).unwrap(),
            Format::Zstd
        );
        #[cfg(feature = "lz4")]
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.lz4")[..]).unwrap(),
            Format::Lz4
        );
    }

    #[test]
    fn test_open() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");

        let mut buffer = Vec::new();
        open("testfiles/plain.txt")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        #[cfg(feature = "brotli")]
        {
            open("testfiles/plain.txt.br")?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "bzip2")]
        {
            open("testfiles/plain.txt.bz2")?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "flate2")]
        {
            open("testfiles/plain.txt.gz")?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "lz4")]
        {
            open("testfiles/plain.txt.lz4")?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "xz2")]
        {
            open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "zstd")]
        {
            open("testfiles/plain.txt.zst")?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        Ok(())
    }

    #[test]
    fn test_open_or_stdin() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");

        let mut buffer = Vec::new();
        open("testfiles/plain.txt")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        #[cfg(feature = "brotli")]
        {
            open_or_stdin(Some("testfiles/plain.txt.br"))?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "bzip2")]
        {
            open_or_stdin(Some("testfiles/plain.txt.bz2"))?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "flate2")]
        {
            open_or_stdin(Some("testfiles/plain.txt.gz"))?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "lz4")]
        {
            open_or_stdin(Some("testfiles/plain.txt.lz4"))?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "xz2")]
        {
            open_or_stdin(Some("testfiles/plain.txt.xz"))?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        buffer.clear();
        #[cfg(feature = "zstd")]
        {
            open_or_stdin(Some("testfiles/plain.txt.zst"))?.read_to_end(&mut buffer)?;
            assert_eq!(buffer, expected_bytes);
        }
        Ok(())
    }

    use temp_testdir::TempDir;

    #[cfg(feature = "flate2")]
    #[test]
    fn test_write_flate2() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt.gz"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt.gz"))?, Format::Gzip)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "xz2")]
    #[test]
    fn test_write_xz() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt.xz"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt.xz"))?, Format::Xz)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "bzip2")]
    #[test]
    fn test_write_bzip2() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt.bz2"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt.bz2"))?, Format::Bzip2)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_write_zstd() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt.zst"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt.zst"))?, Format::Zstd)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "brotli")]
    #[test]
    fn test_write_brotli() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt.br"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt.br"))?, Format::Brotli)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_write_lz4() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt.lz4"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt.lz4"))?, Format::Lz4)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[test]
    fn test_write_plain() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::<u8>::new();

        let mut writer = create(temp.join("plain.txt"), CompressionLevel::Default)?;
        writer.write_all(&expected_bytes[..])?;
        std::mem::drop(writer);
        Decoder::new(fs::File::open(temp.join("plain.txt"))?, Format::Unknown)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }
}
