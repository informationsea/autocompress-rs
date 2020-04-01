use super::Format;
use std::io;

/// structure of decoding reader
pub struct Decoder<R: io::Read> {
    inner: DecoderInner<R>,
}

enum DecoderInner<R: io::Read> {
    Plain(R),
    GzipDecoder(flate2::read::MultiGzDecoder<R>),
    ZlibDecoder(flate2::read::ZlibDecoder<R>),
    Bzip2Decoder(bzip2::read::BzDecoder<R>),
    XzDecoder(xz2::read::XzDecoder<R>),
    SnappyDecoder(snap::read::FrameDecoder<R>),
    ZstdDecoder(zstd::Decoder<io::BufReader<R>>),
    Lz4Decoder(lz4::Decoder<R>),
    BrotliDecoder(brotli::reader::Decompressor<R>),
}

impl<R: io::Read> io::Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            DecoderInner::Plain(x) => x.read(buf),
            DecoderInner::GzipDecoder(x) => x.read(buf),
            DecoderInner::ZlibDecoder(x) => x.read(buf),
            DecoderInner::Bzip2Decoder(x) => x.read(buf),
            DecoderInner::XzDecoder(x) => x.read(buf),
            DecoderInner::SnappyDecoder(x) => x.read(buf),
            DecoderInner::ZstdDecoder(x) => x.read(buf),
            DecoderInner::Lz4Decoder(x) => x.read(buf),
            DecoderInner::BrotliDecoder(x) => x.read(buf),
        }
    }
}

impl<R: io::Read> Decoder<R> {
    /// Create new decoder with a format
    /// ```
    /// # use autocompress::*;
    /// # use std::io::{self, Read};
    /// #
    /// # fn main() -> io::Result<()> {
    /// let mut buffer = Vec::new();
    /// Decoder::new(std::fs::File::open("testfiles/plain.txt.xz")?, Format::Xz)?.
    ///   read_to_end(&mut buffer)?;
    /// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(reader: R, format: Format) -> io::Result<Decoder<R>> {
        Ok(Decoder {
            inner: match format {
                Format::Unknown => DecoderInner::Plain(reader),
                Format::Gzip => {
                    DecoderInner::GzipDecoder(flate2::read::MultiGzDecoder::new(reader))
                }
                Format::Zlib => DecoderInner::ZlibDecoder(flate2::read::ZlibDecoder::new(reader)),
                Format::Bzip2 => DecoderInner::Bzip2Decoder(bzip2::read::BzDecoder::new(reader)),
                Format::Xz => DecoderInner::XzDecoder(xz2::read::XzDecoder::new(reader)),
                Format::Snappy => {
                    DecoderInner::SnappyDecoder(snap::read::FrameDecoder::new(reader))
                }
                Format::Zstd => DecoderInner::ZstdDecoder(zstd::Decoder::new(reader)?),
                Format::Lz4 => DecoderInner::Lz4Decoder(lz4::Decoder::new(reader)?),
                Format::Brotli => {
                    DecoderInner::BrotliDecoder(brotli::reader::Decompressor::new(reader, 1000))
                }
            },
        })
    }
}

impl<R: io::BufRead> Decoder<R> {
    /// Create new decoder with automatically selected decompress.
    /// File format is suggested from magic bytes.
    ///
    /// ```
    /// # use autocompress::*;
    /// # use std::io::{self, Read};
    /// #
    /// # fn main() -> io::Result<()> {
    /// let mut buffer = Vec::new();
    /// Decoder::suggest(io::BufReader::new(std::fs::File::open("testfiles/plain.txt.lz4")?))?.
    ///   read_to_end(&mut buffer)?;
    /// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
    /// # Ok(())
    /// # }
    /// ```
    pub fn suggest(mut reader: R) -> io::Result<Decoder<R>> {
        let format = super::suggest_format(&mut reader)?;
        Decoder::new(reader, format)
    }

    // pub fn with_buffer(reader: R, format: Format) -> io::Result<Decoder<R>> {
    //     Ok(Decoder {
    //         inner: match format {
    //             Format::Unknown => DecoderInner::Plain(reader),
    //             Format::Gzip => {
    //                 DecoderInner::GzipDecoder(flate2::read::MultiGzDecoder::new(reader))
    //             }
    //             Format::Zlib => DecoderInner::ZlibDecoder(flate2::read::ZlibDecoder::new(reader)),
    //             Format::Bzip2 => DecoderInner::Bzip2Decoder(bzip2::read::BzDecoder::new(reader)),
    //             Format::Xz => DecoderInner::XzDecoder(xz2::read::XzDecoder::new(reader)),
    //             Format::Snappy => {
    //                 DecoderInner::SnappyDecoder(snap::read::FrameDecoder::new(reader))
    //             }
    //             Format::Zstd => DecoderInner::ZstdDecoder(zstd::Decoder::new(reader)?),
    //             Format::Lz4 => DecoderInner::Lz4Decoder(lz4::Decoder::new(reader)?),
    //             Format::Brotli => {
    //                 DecoderInner::BrotliDecoder(brotli::reader::Decompressor::new(reader, 1000))
    //             }
    //         },
    //     })
    // }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::prelude::*;

    #[test]
    fn test_decode() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::new();

        buffer.clear();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.bz2")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.gz")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.lz4")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.xz")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.zst")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);

        buffer.clear();
        Decoder::new(
            &include_bytes!("../testfiles/plain.txt.br")[..],
            Format::Brotli,
        )?
        .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }
}
