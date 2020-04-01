use super::Format;
use std::io;

/// List of compression levels
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompressionLevel {
    Fast,
    Default,
    Best,
}

impl Into<flate2::Compression> for CompressionLevel {
    fn into(self) -> flate2::Compression {
        match self {
            CompressionLevel::Fast => flate2::Compression::fast(),
            CompressionLevel::Default => flate2::Compression::default(),
            CompressionLevel::Best => flate2::Compression::best(),
        }
    }
}

impl Into<bzip2::Compression> for CompressionLevel {
    fn into(self) -> bzip2::Compression {
        match self {
            CompressionLevel::Fast => bzip2::Compression::Fastest,
            CompressionLevel::Default => bzip2::Compression::Default,
            CompressionLevel::Best => bzip2::Compression::Best,
        }
    }
}

impl CompressionLevel {
    fn xz_level(self) -> u32 {
        match self {
            CompressionLevel::Fast => 0,
            CompressionLevel::Default => 6,
            CompressionLevel::Best => 9,
        }
    }

    fn zstd_level(self) -> i32 {
        match self {
            CompressionLevel::Fast => 1,
            CompressionLevel::Default => 0,
            CompressionLevel::Best => 21,
        }
    }

    fn lz4_level(self) -> u32 {
        match self {
            CompressionLevel::Fast => 1,
            CompressionLevel::Default => 0,
            CompressionLevel::Best => 12,
        }
    }

    fn brotli(self) -> u32 {
        match self {
            CompressionLevel::Fast => 1,
            CompressionLevel::Default => 11,
            CompressionLevel::Best => 11,
        }
    }
}

/// structure of encoding writer
pub struct Encoder<W: io::Write> {
    inner: EncoderInner<W>,
}

enum EncoderInner<W: io::Write> {
    Plain(W),
    Gzip(flate2::write::GzEncoder<W>),
    Zlib(flate2::write::ZlibEncoder<W>),
    Bzip2(bzip2::write::BzEncoder<W>),
    Xz(xz2::write::XzEncoder<W>),
    Zstd(zstd::stream::write::AutoFinishEncoder<W>),
    Snappy(snap::write::FrameEncoder<W>),
    Lz4(lz4::Encoder<W>),
    Brotli(brotli::enc::writer::CompressorWriter<W>),
}

impl<W: io::Write> io::Write for Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            EncoderInner::Plain(x) => x.write(buf),
            EncoderInner::Gzip(x) => x.write(buf),
            EncoderInner::Zlib(x) => x.write(buf),
            EncoderInner::Bzip2(x) => x.write(buf),
            EncoderInner::Xz(x) => x.write(buf),
            EncoderInner::Zstd(x) => x.write(buf),
            EncoderInner::Snappy(x) => x.write(buf),
            EncoderInner::Lz4(x) => x.write(buf),
            EncoderInner::Brotli(x) => x.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            EncoderInner::Plain(x) => x.flush(),
            EncoderInner::Gzip(x) => x.flush(),
            EncoderInner::Zlib(x) => x.flush(),
            EncoderInner::Bzip2(x) => x.flush(),
            EncoderInner::Xz(x) => x.flush(),
            EncoderInner::Zstd(x) => x.flush(),
            EncoderInner::Snappy(x) => x.flush(),
            EncoderInner::Lz4(x) => x.flush(),
            EncoderInner::Brotli(x) => x.flush(),
        }
    }
}

impl<W: io::Write> Encoder<W> {
    /// create new encoder with a writer, a format and a compression level.
    ///
    /// ```
    /// # use autocompress::*;
    /// # use std::io::{self, Write};
    /// #
    /// # fn main() -> io::Result<()> {
    /// let mut encoder = Encoder::new(
    ///     std::fs::File::create("new-file.txt.gz")?,
    ///     Format::Gzip,
    ///     CompressionLevel::Default
    /// )?;
    /// encoder.write_all(b"hello, world")?;
    /// # std::fs::remove_file("new-file.txt.gz")?;
    /// #  Ok(())
    /// # }
    /// ```
    pub fn new(writer: W, format: Format, compression_level: CompressionLevel) -> io::Result<Self> {
        Ok(Encoder {
            inner: match format {
                Format::Unknown => EncoderInner::Plain(writer),
                Format::Gzip => EncoderInner::Gzip(flate2::write::GzEncoder::new(
                    writer,
                    compression_level.into(),
                )),
                Format::Zlib => EncoderInner::Zlib(flate2::write::ZlibEncoder::new(
                    writer,
                    compression_level.into(),
                )),
                Format::Bzip2 => EncoderInner::Bzip2(bzip2::write::BzEncoder::new(
                    writer,
                    compression_level.into(),
                )),
                Format::Xz => EncoderInner::Xz(xz2::write::XzEncoder::new(
                    writer,
                    compression_level.xz_level(),
                )),
                Format::Zstd => EncoderInner::Zstd(
                    zstd::Encoder::new(writer, compression_level.zstd_level())?.auto_finish(),
                ),
                Format::Snappy => EncoderInner::Snappy(snap::write::FrameEncoder::new(writer)),
                Format::Lz4 => EncoderInner::Lz4(
                    lz4::EncoderBuilder::new()
                        .level(compression_level.lz4_level())
                        .auto_flush(true)
                        .build(writer)?,
                ),
                Format::Brotli => EncoderInner::Brotli(brotli::enc::writer::CompressorWriter::new(
                    writer,
                    1024 * 10,
                    compression_level.brotli(),
                    22,
                )),
            },
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Decoder;
    use std::io::prelude::*;
    #[test]
    fn test_encoder() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");

        let mut buffer: Vec<u8> = Vec::new();
        Encoder::new(&mut buffer, Format::Unknown, CompressionLevel::Default)?
            .write_all(expected_bytes)?;
        assert_eq!(buffer, expected_bytes);

        let mut read_buffer = Vec::new();

        for one_format in &[
            Format::Zlib,
            Format::Gzip,
            Format::Bzip2,
            Format::Snappy,
            Format::Xz,
            Format::Brotli,
            Format::Lz4,
            Format::Zstd,
        ] {
            println!("processing: {:?}", one_format);
            buffer.clear();
            read_buffer.clear();
            {
                let mut encoder =
                    Encoder::new(&mut buffer, *one_format, CompressionLevel::Default)?;
                encoder.write_all(expected_bytes)?;
                encoder.flush()?;
            }
            Decoder::new(&buffer[..], *one_format)?.read_to_end(&mut read_buffer)?;
            assert_eq!(read_buffer, expected_bytes);
        }

        Ok(())
    }
}
