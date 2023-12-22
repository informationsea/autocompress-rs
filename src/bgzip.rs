//! [bgzip](https://github.com/informationsea/bgzip-rs) format support
//!
//! Specification of this format is available at [htslib webpage](https://samtools.github.io/hts-specs/SAMv1.pdf).
//!
//! BgzipDecompress is not available in this crate because normal GzipDecompress can decompress bgzip files.
//! If you need seek support for bgzip, please use [bgzip-rs](https://github.com/informationsea/bgzip-rs).

use crate::{Error, Flush, Processor, Result, Status};

pub type BgzipCompressWriter<R> = crate::io::ProcessorWriter<BgzipCompress, R>;

/// `AsyncBgzipCompressWriter` is a struct that allows compression of data using the BGZF format.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncWriteExt;
/// # use tokio::fs::File;
/// # use autocompress::bgzip::AsyncBgzipCompressWriter;
/// #
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/doc-bgzip-compress-async-writer.gz").await?;
/// let mut bgzip_writer = AsyncBgzipCompressWriter::new(file_writer);
/// bgzip_writer.write_all(&b"Hello, world"[..]).await?;
/// bgzip_writer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub type AsyncBgzipCompressWriter<W> = crate::io::AsyncProcessorWriter<BgzipCompress, W>;

pub struct BgzipCompress {
    compress: bgzip::deflate::Compress,
    unprocessed_input: Vec<u8>,
    unprocessed_output: Vec<u8>,
    compress_unit_size: usize,
    is_stream_end: bool,
    total_in: u64,
    total_out: u64,
}

impl Default for BgzipCompress {
    fn default() -> Self {
        Self::new(bgzip::deflate::Compression::default())
    }
}

impl BgzipCompress {
    pub fn new(level: bgzip::deflate::Compression) -> Self {
        Self {
            compress: bgzip::deflate::Compress::new(level),
            unprocessed_input: Vec::new(),
            unprocessed_output: Vec::new(),
            compress_unit_size: bgzip::write::DEFAULT_COMPRESS_UNIT_SIZE,
            is_stream_end: false,
            total_in: 0,
            total_out: 0,
        }
    }

    fn write_unprocessed_output<'a>(&mut self, mut output: &'a mut [u8]) -> &'a mut [u8] {
        while !self.unprocessed_output.is_empty() && !output.is_empty() {
            let len = std::cmp::min(self.unprocessed_output.len(), output.len());
            output[..len].copy_from_slice(&self.unprocessed_output[..len]);
            self.unprocessed_output.drain(..len);
            output = &mut output[len..];
            self.total_out += TryInto::<u64>::try_into(len).unwrap();
        }
        output
    }

    fn load_unprocessed_input<'a>(&mut self, mut input: &'a [u8]) -> &'a [u8] {
        while self.unprocessed_input.len() < self.compress_unit_size && !input.is_empty() {
            let len = std::cmp::min(
                self.compress_unit_size - self.unprocessed_input.len(),
                input.len(),
            );
            // dbg!(len, input.len(), self.unprocessed_input.len());
            self.unprocessed_input.extend_from_slice(&input[..len]);
            input = &input[len..];
            self.total_in += TryInto::<u64>::try_into(len).unwrap();
        }
        input
    }
}

impl Processor for BgzipCompress {
    fn process(&mut self, mut input: &[u8], mut output: &mut [u8], flush: Flush) -> Result<Status> {
        output = self.write_unprocessed_output(output);
        if output.is_empty() {
            return Ok(Status::Ok);
        }
        input = self.load_unprocessed_input(input);

        loop {
            if self.unprocessed_input.len() < self.compress_unit_size && flush == Flush::None {
                return Ok(Status::Ok);
            }

            if !self.unprocessed_input.is_empty() {
                bgzip::write::write_block(
                    &mut self.unprocessed_output,
                    &self.unprocessed_input,
                    &mut self.compress,
                )
                .map_err(|e| Error::CompressError(e.to_string()))?;
                self.unprocessed_input.clear();
                input = self.load_unprocessed_input(input);
            }

            output = self.write_unprocessed_output(output);

            if self.unprocessed_output.is_empty() && self.unprocessed_input.is_empty() {
                break;
            }
            if output.is_empty() {
                return Ok(Status::Ok);
            }
        }

        // dbg!(flush, self.is_stream_end);

        if flush == Flush::Finish
            && self.unprocessed_output.is_empty()
            && self.unprocessed_input.is_empty()
            && !self.is_stream_end
        {
            self.unprocessed_output
                .extend_from_slice(&bgzip::EOF_MARKER);
            self.write_unprocessed_output(output);
            self.is_stream_end = true;
        }

        // if self.is_stream_end {
        //     dbg!(
        //         self.unprocessed_output.len(),
        //         self.unprocessed_input.len(),
        //         input.len(),
        //         output.len(),
        //         self.total_in,
        //         self.total_out
        //     );
        // }

        if self.is_stream_end
            && self.unprocessed_output.is_empty()
            && self.unprocessed_input.is_empty()
        {
            Ok(Status::StreamEnd)
        } else {
            Ok(Status::Ok)
        }
    }

    fn reset(&mut self) {
        self.unprocessed_input.clear();
        self.unprocessed_output.clear();
        self.total_in = 0;
        self.total_out = 0;
        self.is_stream_end = false;
    }

    fn total_in(&self) -> u64 {
        self.total_in
    }

    fn total_out(&self) -> u64 {
        self.total_out
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};

    use super::*;

    #[test]
    fn test_bgzip() -> anyhow::Result<()> {
        let data = include_bytes!("../testfiles/pg2701.txt");
        let mut result = std::fs::File::create("target/test.bgzip.gz")?;
        let mut buffer = vec![0; 10_000_000];
        let mut compress = BgzipCompress::new(bgzip::deflate::Compression::default());
        let status = compress.process(&data[..], &mut buffer, Flush::Finish)?;
        assert_eq!(status, Status::StreamEnd);
        result.write_all(&buffer[..compress.total_out() as usize])?;

        let mut decompress_reader =
            ::flate2::read::MultiGzDecoder::new(&buffer[..compress.total_out() as usize]);
        let mut decompressed_data = Vec::new();
        decompress_reader.read_to_end(&mut decompressed_data)?;
        assert_eq!(decompressed_data.len(), data.len());
        assert_eq!(decompressed_data, data);

        Ok(())
    }

    #[test]
    fn test_bgzip_compress_small_step() -> anyhow::Result<()> {
        let data = include_bytes!("../testfiles/pg2701.txt");
        let mut compressed_buffer: Vec<u8> = vec![0u8; 10_000_000];
        let mut compress = BgzipCompress::new(bgzip::deflate::Compression::default());

        // small step input
        while compress.total_in() < data.len() as u64 {
            let input = &data
                [compress.total_in() as usize..(compress.total_in() as usize + 7).min(data.len())];
            let output = &mut compressed_buffer[compress.total_out() as usize..];
            assert_eq!(compress.process(input, output, Flush::None)?, Status::Ok);
        }
        let output = &mut compressed_buffer[compress.total_out() as usize..];
        assert_eq!(
            compress.process(&[], output, Flush::Finish)?,
            Status::StreamEnd
        );

        let mut decompress_reader = ::flate2::read::MultiGzDecoder::new(
            &compressed_buffer[..compress.total_out() as usize],
        );
        let mut decompressed_data = Vec::new();
        decompress_reader.read_to_end(&mut decompressed_data)?;
        assert_eq!(decompressed_data.len(), data.len());
        assert_eq!(decompressed_data, data);

        // small step output
        compress.reset();
        while compress.total_in() < data.len() as u64 {
            let input = &data[compress.total_in() as usize..];
            let output = &mut compressed_buffer
                [compress.total_out() as usize..(compress.total_out() as usize + 7)];
            assert_eq!(compress.process(input, output, Flush::None)?, Status::Ok);
        }

        loop {
            let output = &mut compressed_buffer
                [compress.total_out() as usize..(compress.total_out() as usize + 7)];
            // dbg!(
            //     compress.total_out(),
            //     compress.unprocessed_output.len(),
            //     compress.is_stream_end
            // );

            match compress.process(&[], output, Flush::Finish)? {
                Status::Ok => {}
                Status::StreamEnd => break,
            }
            //panic!()
        }

        let mut decompress_reader = ::flate2::read::MultiGzDecoder::new(
            &compressed_buffer[..compress.total_out() as usize],
        );
        let mut decompressed_data = Vec::new();
        decompress_reader.read_to_end(&mut decompressed_data)?;
        assert_eq!(decompressed_data.len(), data.len());
        assert_eq!(decompressed_data, data);

        Ok(())
    }
}
