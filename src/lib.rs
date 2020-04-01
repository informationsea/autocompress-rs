//! Automatically select suitable decoder from magic bytes or encoder from file extension.
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
//! Example
//! ```
//! use autocompress::open;
//! use std::io::{self, Read};
//!
//! fn main() -> io::Result<()> {
//!   let mut buffer = Vec::new();
//!   open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
//!   assert_eq!(buffer, b"ABCDEFG\r\n1234567");
//!   Ok(())
//! }

mod read;
mod write;

pub use read::Decoder;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, prelude::*};
use std::path;
pub use write::{CompressionLevel, Encoder};

/// List of supported file formats
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Format {
    Gzip,
    Zlib,
    Bzip2,
    Xz,
    Snappy,
    Zstd,
    Lz4,
    Brotli,
    Unknown,
}

/// Suggest file format from magic bytes.
/// This function does not consume data.
pub fn suggest_format(mut reader: impl io::BufRead) -> io::Result<Format> {
    let buffer = reader.fill_buf()?;
    if buffer.starts_with(&[0x1f, 0x8b]) {
        Ok(Format::Gzip)
    } else if buffer.starts_with(b"BZ") {
        Ok(Format::Bzip2)
    } else if buffer.starts_with(b"sNaPpY") {
        Ok(Format::Snappy)
    } else if buffer.starts_with(&[0xFD, b'7', b'z', b'X', b'Z', 0x00]) {
        Ok(Format::Xz)
    } else if buffer.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        Ok(Format::Zstd)
    } else if buffer.starts_with(&[0x04, 0x22, 0x4D, 0x18]) {
        Ok(Format::Lz4)
    } else {
        Ok(Format::Unknown)
    }
}

/// Suggest file format from a path extension
pub fn suggest_format_from_path(path: impl AsRef<path::Path>) -> Format {
    if path.as_ref().extension() == Some(OsStr::new("br")) {
        Format::Brotli
    } else if path.as_ref().extension() == Some(OsStr::new("gz")) {
        Format::Gzip
    } else if path.as_ref().extension() == Some(OsStr::new("bz2")) {
        Format::Bzip2
    } else if path.as_ref().extension() == Some(OsStr::new("xz")) {
        Format::Xz
    } else if path.as_ref().extension() == Some(OsStr::new("zst")) {
        Format::Zstd
    } else if path.as_ref().extension() == Some(OsStr::new("lz4")) {
        Format::Lz4
    } else {
        Format::Unknown
    }
}

/// Open new reader from file path. File format is suggested from magic bytes and file extension.
///
/// ```
/// # use autocompress::*;
/// # use std::io::{self, Read};
/// #
/// # fn main() -> io::Result<()> {
/// let mut buffer = Vec::new();
/// open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
/// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
/// #  Ok(())
/// # }
/// ```
pub fn open(path: impl AsRef<path::Path>) -> io::Result<read::Decoder<io::BufReader<fs::File>>> {
    let mut reader = io::BufReader::new(fs::File::open(&path)?);
    let mut format = suggest_format(&mut reader)?;
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
/// let mut buffer = Vec::new();
/// open_or_stdin(Some("testfiles/plain.txt.xz"))?.read_to_end(&mut buffer)?;
/// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
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
/// let mut writer = create("create.txt.lz4")?;
/// writer.write_all(b"hello, world")?;
/// # std::fs::remove_file("create.txt.lz4")?;
/// #  Ok(())
/// # }
/// ```
pub fn create(path: impl AsRef<path::Path>) -> io::Result<write::Encoder<fs::File>> {
    let format = suggest_format_from_path(&path);
    Encoder::new(fs::File::create(path)?, format, CompressionLevel::Default)
}

/// Create new writer from a file path. If a file path is `None`, standard output is used. File format is suggested from file extension.
///
/// ```
/// # use autocompress::*;
/// # use std::io::{self, Write};
/// #
/// # fn main() -> io::Result<()> {
/// let mut writer = create_or_stdout(Some("create.txt.lz4"))?;
/// writer.write_all(b"hello, world")?;
/// # std::fs::remove_file("create.txt.lz4")?;
/// #  Ok(())
/// # }
/// ```
pub fn create_or_stdout(path: Option<impl AsRef<path::Path>>) -> io::Result<Box<dyn Write>> {
    if let Some(path) = path {
        Ok(Box::new(create(path)?))
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
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.gz")[..]).unwrap(),
            Format::Gzip
        );
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.bz2")[..]).unwrap(),
            Format::Bzip2
        );
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.xz")[..]).unwrap(),
            Format::Xz
        );
        assert_eq!(
            suggest_format(&include_bytes!("../testfiles/plain.txt.zst")[..]).unwrap(),
            Format::Zstd
        );
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
        open("testfiles/plain.txt.br")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open("testfiles/plain.txt.bz2")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open("testfiles/plain.txt.gz")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open("testfiles/plain.txt.lz4")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open("testfiles/plain.txt.zst")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[test]
    fn test_open_or_stdin() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");

        let mut buffer = Vec::new();
        open("testfiles/plain.txt")?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        open_or_stdin(Some("testfiles/plain.txt.br"))?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open_or_stdin(Some("testfiles/plain.txt.bz2"))?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open_or_stdin(Some("testfiles/plain.txt.gz"))?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open_or_stdin(Some("testfiles/plain.txt.lz4"))?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open_or_stdin(Some("testfiles/plain.txt.xz"))?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        buffer.clear();
        open_or_stdin(Some("testfiles/plain.txt.zst"))?.read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    use temp_testdir::TempDir;

    #[test]
    fn test_write() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let temp = TempDir::default();
        let mut buffer = Vec::new();

        {
            let mut writer = create(temp.join("plain.txt.gz"))?;
            writer.write_all(&expected_bytes[..])?;
        }
        Decoder::new(fs::File::open(temp.join("plain.txt.gz"))?, Format::Gzip)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        {
            let mut writer = create(temp.join("plain.txt.br"))?;
            writer.write_all(&expected_bytes[..])?;
        }
        Decoder::new(fs::File::open(temp.join("plain.txt.br"))?, Format::Brotli)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        {
            let mut writer = create(temp.join("plain.txt.bz2"))?;
            writer.write_all(&expected_bytes[..])?;
        }
        Decoder::new(fs::File::open(temp.join("plain.txt.bz2"))?, Format::Bzip2)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        {
            let mut writer = create(temp.join("plain.txt.xz"))?;
            writer.write_all(&expected_bytes[..])?;
        }
        Decoder::new(fs::File::open(temp.join("plain.txt.xz"))?, Format::Xz)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        {
            let mut writer = create(temp.join("plain.txt.zst"))?;
            writer.write_all(&expected_bytes[..])?;
        }
        Decoder::new(fs::File::open(temp.join("plain.txt.zst"))?, Format::Zstd)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        {
            let mut writer = create(temp.join("plain.txt.lz4"))?;
            writer.write_all(&expected_bytes[..])?;
        }
        Decoder::new(fs::File::open(temp.join("plain.txt.lz4"))?, Format::Lz4)?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        Ok(())
    }
}
