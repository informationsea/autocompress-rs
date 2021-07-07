use super::Format;
use std::io;

/// List of compression levels
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompressionLevel {
    Fast,
    Default,
    Best,
    Number(i32),
}

#[cfg(feature = "flate2")]
impl Into<flate2::Compression> for CompressionLevel {
    fn into(self) -> flate2::Compression {
        match self {
            CompressionLevel::Fast => flate2::Compression::fast(),
            CompressionLevel::Default => flate2::Compression::default(),
            CompressionLevel::Best => flate2::Compression::best(),
            CompressionLevel::Number(x) => flate2::Compression::new(x as u32),
        }
    }
}

#[cfg(feature = "bzip2")]
impl Into<bzip2::Compression> for CompressionLevel {
    fn into(self) -> bzip2::Compression {
        match self {
            CompressionLevel::Fast => bzip2::Compression::fast(),
            CompressionLevel::Default => bzip2::Compression::default(),
            CompressionLevel::Best => bzip2::Compression::best(),
            CompressionLevel::Number(x) => bzip2::Compression::new(x as u32),
        }
    }
}

impl CompressionLevel {
    #[cfg(feature = "xz2")]
    fn xz_level(self) -> u32 {
        match self {
            CompressionLevel::Fast => 0,
            CompressionLevel::Default => 6,
            CompressionLevel::Best => 9,
            CompressionLevel::Number(x) => x as u32,
        }
    }

    #[cfg(feature = "zstd")]
    fn zstd_level(self) -> i32 {
        match self {
            CompressionLevel::Fast => 1,
            CompressionLevel::Default => 0,
            CompressionLevel::Best => 21,
            CompressionLevel::Number(x) => x as i32,
        }
    }

    #[cfg(feature = "lz4")]
    fn lz4_level(self) -> u32 {
        match self {
            CompressionLevel::Fast => 1,
            CompressionLevel::Default => 0,
            CompressionLevel::Best => 12,
            CompressionLevel::Number(x) => x as u32,
        }
    }

    #[cfg(feature = "brotli")]
    fn brotli(self) -> u32 {
        match self {
            CompressionLevel::Fast => 1,
            CompressionLevel::Default => 11,
            CompressionLevel::Best => 11,
            CompressionLevel::Number(x) => x as u32,
        }
    }
}

/// structure of encoding writer
pub struct Encoder<W: io::Write> {
    inner: EncoderInner<W>,
}

enum EncoderInner<W: io::Write> {
    Plain(W),
    #[cfg(feature = "flate2")]
    Gzip(flate2::write::GzEncoder<W>),
    #[cfg(feature = "flate2")]
    Zlib(flate2::write::ZlibEncoder<W>),
    #[cfg(feature = "bzip2")]
    Bzip2(bzip2::write::BzEncoder<W>),
    #[cfg(feature = "xz2")]
    Xz(xz2::write::XzEncoder<W>),
    #[cfg(feature = "zstd")]
    Zstd(Option<zstd::stream::write::Encoder<W>>),
    #[cfg(feature = "snap")]
    Snappy(snap::write::FrameEncoder<W>),
    #[cfg(feature = "lz4")]
    Lz4(lz4::Encoder<W>),
    #[cfg(feature = "brotli")]
    Brotli(brotli::enc::writer::CompressorWriter<W>),
}

impl<W: io::Write> io::Write for Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            EncoderInner::Plain(x) => x.write(buf),
            #[cfg(feature = "flate2")]
            EncoderInner::Gzip(x) => x.write(buf),
            #[cfg(feature = "flate2")]
            EncoderInner::Zlib(x) => x.write(buf),
            #[cfg(feature = "bzip2")]
            EncoderInner::Bzip2(x) => x.write(buf),
            #[cfg(feature = "xz2")]
            EncoderInner::Xz(x) => x.write(buf),
            #[cfg(feature = "zstd")]
            EncoderInner::Zstd(x) => x.as_mut().unwrap().write(buf),
            #[cfg(feature = "snap")]
            EncoderInner::Snappy(x) => x.write(buf),
            #[cfg(feature = "lz4")]
            EncoderInner::Lz4(x) => x.write(buf),
            #[cfg(feature = "brotli")]
            EncoderInner::Brotli(x) => x.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            EncoderInner::Plain(x) => x.flush(),
            #[cfg(feature = "flate2")]
            EncoderInner::Gzip(x) => x.flush(),
            #[cfg(feature = "flate2")]
            EncoderInner::Zlib(x) => x.flush(),
            #[cfg(feature = "bzip2")]
            EncoderInner::Bzip2(x) => x.flush(),
            #[cfg(feature = "xz2")]
            EncoderInner::Xz(x) => x.flush(),
            #[cfg(feature = "zstd")]
            EncoderInner::Zstd(x) => x.as_mut().unwrap().flush(),
            #[cfg(feature = "snap")]
            EncoderInner::Snappy(x) => x.flush(),
            #[cfg(feature = "lz4")]
            EncoderInner::Lz4(x) => x.flush(),
            #[cfg(feature = "brotli")]
            EncoderInner::Brotli(x) => x.flush(),
        }
    }
}

#[cfg(feature = "zstd")]
impl<W: io::Write> Drop for Encoder<W> {
    fn drop(&mut self) {
        if let EncoderInner::Zstd(x) = &mut self.inner {
            x.take()
                .unwrap()
                .finish()
                .expect("Failed to finish zstd stream");
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
    /// # #[cfg(feature = "flate2")] {
    /// let mut encoder = Encoder::new(
    ///     std::fs::File::create("new-file.txt.gz")?,
    ///     Format::Gzip,
    ///     CompressionLevel::Default
    /// )?;
    /// encoder.write_all(b"hello, world")?;
    /// # std::fs::remove_file("new-file.txt.gz")?;
    /// # }
    /// #  Ok(())
    /// # }
    /// ```
    pub fn new(writer: W, format: Format, compression_level: CompressionLevel) -> io::Result<Self> {
        Ok(Encoder {
            inner: match format {
                Format::Unknown => EncoderInner::Plain(writer),
                #[cfg(feature = "flate2")]
                Format::Gzip => EncoderInner::Gzip(flate2::write::GzEncoder::new(
                    writer,
                    compression_level.into(),
                )),
                #[cfg(feature = "flate2")]
                Format::Zlib => EncoderInner::Zlib(flate2::write::ZlibEncoder::new(
                    writer,
                    compression_level.into(),
                )),
                #[cfg(feature = "bzip2")]
                Format::Bzip2 => EncoderInner::Bzip2(bzip2::write::BzEncoder::new(
                    writer,
                    compression_level.into(),
                )),
                #[cfg(feature = "xz2")]
                Format::Xz => EncoderInner::Xz(xz2::write::XzEncoder::new(
                    writer,
                    compression_level.xz_level(),
                )),
                #[cfg(feature = "zstd")]
                Format::Zstd => EncoderInner::Zstd(Some(zstd::Encoder::new(
                    writer,
                    compression_level.zstd_level(),
                )?)),
                #[cfg(feature = "snap")]
                Format::Snappy => EncoderInner::Snappy(snap::write::FrameEncoder::new(writer)),
                #[cfg(feature = "lz4")]
                Format::Lz4 => EncoderInner::Lz4(
                    lz4::EncoderBuilder::new()
                        .level(compression_level.lz4_level())
                        .auto_flush(true)
                        .build(writer)?,
                ),
                #[cfg(feature = "brotli")]
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
            #[cfg(feature = "flate2")]
            Format::Zlib,
            #[cfg(feature = "flate2")]
            Format::Gzip,
            #[cfg(feature = "bzip2")]
            Format::Bzip2,
            #[cfg(feature = "snap")]
            Format::Snappy,
            #[cfg(feature = "xz2")]
            Format::Xz,
            #[cfg(feature = "brotli")]
            Format::Brotli,
            #[cfg(feature = "lz4")]
            Format::Lz4,
            #[cfg(feature = "zstd")]
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
