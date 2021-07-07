use super::Format;
use std::io;

/// structure of decoding reader
pub struct Decoder<R: io::Read> {
    inner: DecoderInner<R>,
}

enum DecoderInner<R: io::Read> {
    Plain(R),
    #[cfg(feature = "flate2")]
    GzipDecoder(flate2::read::MultiGzDecoder<R>),
    #[cfg(feature = "flate2")]
    ZlibDecoder(flate2::read::ZlibDecoder<R>),
    #[cfg(feature = "bzip2")]
    Bzip2Decoder(bzip2::read::BzDecoder<R>),
    #[cfg(feature = "xz2")]
    XzDecoder(xz2::read::XzDecoder<R>),
    #[cfg(feature = "snap")]
    SnappyDecoder(snap::read::FrameDecoder<R>),
    #[cfg(feature = "zstd")]
    ZstdDecoder(zstd::Decoder<'static, io::BufReader<R>>),
    #[cfg(feature = "lz4")]
    Lz4Decoder(lz4::Decoder<R>),
    #[cfg(feature = "brotli")]
    BrotliDecoder(brotli::reader::Decompressor<R>),
}

impl<R: io::Read> io::Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            DecoderInner::Plain(x) => x.read(buf),
            #[cfg(feature = "flate2")]
            DecoderInner::GzipDecoder(x) => x.read(buf),
            #[cfg(feature = "flate2")]
            DecoderInner::ZlibDecoder(x) => x.read(buf),
            #[cfg(feature = "bzip2")]
            DecoderInner::Bzip2Decoder(x) => x.read(buf),
            #[cfg(feature = "xz2")]
            DecoderInner::XzDecoder(x) => x.read(buf),
            #[cfg(feature = "snap")]
            DecoderInner::SnappyDecoder(x) => x.read(buf),
            #[cfg(feature = "zstd")]
            DecoderInner::ZstdDecoder(x) => x.read(buf),
            #[cfg(feature = "lz4")]
            DecoderInner::Lz4Decoder(x) => x.read(buf),
            #[cfg(feature = "brotli")]
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
    /// # #[cfg(feature = "xz2")] {
    /// let mut buffer = Vec::new();
    /// Decoder::new(std::fs::File::open("testfiles/plain.txt.xz")?, Format::Xz)?.
    ///   read_to_end(&mut buffer)?;
    /// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
    /// # }
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(reader: R, format: Format) -> io::Result<Decoder<R>> {
        Ok(Decoder {
            inner: match format {
                Format::Unknown => DecoderInner::Plain(reader),
                #[cfg(feature = "flate2")]
                Format::Gzip => {
                    DecoderInner::GzipDecoder(flate2::read::MultiGzDecoder::new(reader))
                }
                #[cfg(feature = "flate2")]
                Format::Zlib => DecoderInner::ZlibDecoder(flate2::read::ZlibDecoder::new(reader)),
                #[cfg(feature = "bzip2")]
                Format::Bzip2 => DecoderInner::Bzip2Decoder(bzip2::read::BzDecoder::new(reader)),
                #[cfg(feature = "xz2")]
                Format::Xz => DecoderInner::XzDecoder(xz2::read::XzDecoder::new(reader)),
                #[cfg(feature = "snap")]
                Format::Snappy => {
                    DecoderInner::SnappyDecoder(snap::read::FrameDecoder::new(reader))
                }
                #[cfg(feature = "zstd")]
                Format::Zstd => DecoderInner::ZstdDecoder(zstd::Decoder::new(reader)?),
                #[cfg(feature = "lz4")]
                Format::Lz4 => DecoderInner::Lz4Decoder(lz4::Decoder::new(reader)?),
                #[cfg(feature = "brotli")]
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
    /// # #[cfg(feature = "xz2")] {
    /// let mut buffer = Vec::new();
    /// Decoder::suggest(io::BufReader::new(std::fs::File::open("testfiles/plain.txt.lz4")?))?.
    ///   read_to_end(&mut buffer)?;
    /// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
    /// # }
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

    #[cfg(feature = "bzip2")]
    #[test]
    fn test_decode_bzip2() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.bz2")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "flate2")]
    #[test]
    fn test_decode_flate2() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.gz")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_decode_lz4() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.lz4")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "xz2")]
    #[test]
    fn test_decode_xz() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.xz")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_decode_zstd() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.zst")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[cfg(feature = "botrli")]
    #[test]
    fn test_decode_botrli() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt.br")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }

    #[test]
    fn test_decode_plain() -> io::Result<()> {
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut buffer = Vec::<u8>::new();
        Decoder::suggest(&include_bytes!("../testfiles/plain.txt")[..])?
            .read_to_end(&mut buffer)?;
        assert_eq!(buffer, expected_bytes);
        Ok(())
    }
}
