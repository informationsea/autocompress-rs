use anyhow::Context;
use std::io::prelude::*;

#[cfg(feature = "tokio")]
use tokio::fs::File;
#[cfg(feature = "tokio")]
use tokio::io::AsyncRead;
#[cfg(feature = "tokio")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "tokio")]
use tokio::io::BufReader;
#[cfg(feature = "tokio")]
use tokio::pin;

use super::*;

pub(crate) fn test_compress<
    P1: Processor,
    F1: Fn() -> anyhow::Result<P1>,
    P2: Processor,
    F2: Fn() -> anyhow::Result<P2>,
>(
    compress_generator: F1,
    decompress_generator: F2,
) -> anyhow::Result<()> {
    let data = include_bytes!("../testfiles/pg2701.txt");
    let mut buffer = vec![0; 10_000_000];

    // compress at once
    let mut compress = compress_generator()?;
    assert_eq!(
        compress
            .process(&data[..], &mut buffer, Flush::Finish)
            .context("Compress process")?,
        Status::StreamEnd
    );
    assert_eq!(compress.total_in(), data.len() as u64);
    let mut decompress = decompress_generator()?;
    let mut buffer2 = vec![0; 10_000_000];
    assert_eq!(
        decompress
            .process(
                &buffer[..(compress.total_out() as usize)],
                &mut buffer2,
                Flush::Finish
            )
            .context("Decompress process")?,
        Status::StreamEnd
    );
    assert_eq!(&buffer2[..(decompress.total_out() as usize)], &data[..]);

    // compress step by step (input)
    let mut compress = compress_generator()?;
    while compress.total_in() < data.len().try_into().unwrap() {
        let input_buf = &data[TryInto::<usize>::try_into(compress.total_in()).unwrap()
            ..TryInto::<usize>::try_into(compress.total_in() + 1001)
                .unwrap()
                .min(data.len())];
        assert_eq!(
            compress
                .process(
                    input_buf,
                    &mut buffer[TryInto::<usize>::try_into(compress.total_out()).unwrap()..],
                    Flush::None
                )
                .context("Compress process")?,
            Status::Ok
        );
    }
    assert_eq!(
        compress
            .process(
                &[],
                &mut buffer[TryInto::<usize>::try_into(compress.total_out()).unwrap()..],
                Flush::Finish
            )
            .context("Finalize process")?,
        Status::StreamEnd
    );

    assert_eq!(compress.total_in(), data.len() as u64);
    let mut decompress = decompress_generator()?;
    let mut buffer2 = vec![0; 10_000_000];
    assert_eq!(
        decompress
            .process(
                &buffer[..(compress.total_out() as usize)],
                &mut buffer2,
                Flush::Finish
            )
            .context("Decompress process")?,
        Status::StreamEnd
    );
    assert_eq!(&buffer2[..(decompress.total_out() as usize)], &data[..]);

    // compress step by step (output)
    let mut compress = compress_generator()?;
    while compress.total_in() < data.len().try_into().unwrap() {
        //dbg!(compress.total_in(), compress.total_out());
        let input_buf = &data[TryInto::<usize>::try_into(compress.total_in()).unwrap()..];
        let output_buf = &mut buffer[TryInto::<usize>::try_into(compress.total_out()).unwrap()
            ..TryInto::<usize>::try_into(compress.total_out() + 1001).unwrap()];
        assert_eq!(
            compress
                .process(input_buf, output_buf, Flush::None)
                .context("Compress process")?,
            Status::Ok
        );
    }
    loop {
        //dbg!(compress.total_in(), compress.total_out());
        let output_buf = &mut buffer[TryInto::<usize>::try_into(compress.total_out()).unwrap()
            ..TryInto::<usize>::try_into(compress.total_out() + 1001).unwrap()];
        let status = compress
            .process(&[], output_buf, Flush::Finish)
            .context("Finalize process")?;
        match status {
            Status::Ok => {}
            Status::StreamEnd => break,
        }
    }
    //dbg!(compress.total_in(), compress.total_out());

    assert_eq!(compress.total_in(), data.len() as u64);
    let mut decompress = decompress_generator()?;
    let mut buffer2 = vec![0; 10_000_000];
    assert_eq!(
        decompress
            .process(
                &buffer[..(compress.total_out() as usize)],
                &mut buffer2,
                Flush::Finish
            )
            .context("Decompress process")?,
        Status::StreamEnd
    );
    assert_eq!(&buffer2[..(decompress.total_out() as usize)], &data[..]);

    Ok(())
}

pub(crate) fn test_decompress<D: Processor>(
    mut decompress: D,
    data: &[u8],
) -> crate::error::Result<()> {
    let expected_data = include_bytes!("../testfiles/pg2701.txt");
    let mut output = vec![0u8; 8800000];
    let mut total_in = 0;
    let mut total_out = 0;
    while total_in < data.len().try_into().unwrap() {
        assert_eq!(
            decompress.process(
                &data[TryInto::<usize>::try_into(total_in).unwrap()..],
                &mut output[TryInto::<usize>::try_into(total_out).unwrap()..],
                Flush::Finish
            )?,
            Status::StreamEnd
        );
        total_in += decompress.total_in();
        total_out += decompress.total_out();
        decompress.reset();
    }
    assert_eq!(total_in, data.len() as u64);
    assert_eq!(total_out, expected_data.len() as u64);
    assert_eq!(&output[..expected_data.len()], &expected_data[..]);

    // decompress step by step
    let process_step = 7;
    decompress.reset();
    let mut output = vec![0u8; 8800000];
    let mut total_in = 0;
    let mut total_out = 0;
    while total_in < data.len().try_into().unwrap() {
        let start_point = total_in as usize;
        let original_total_in = decompress.total_in();
        let original_total_out = decompress.total_out();
        match decompress.process(
            &data[start_point..(start_point + process_step).min(data.len())],
            &mut output[(total_out as usize)..],
            Flush::None,
        )? {
            Status::StreamEnd => {
                total_in += decompress.total_in() - original_total_in;
                total_out += decompress.total_out() - original_total_out;
                decompress.reset()
            }
            Status::Ok => {
                total_in += decompress.total_in() - original_total_in;
                total_out += decompress.total_out() - original_total_out;
            }
        }
    }
    assert_eq!(total_in, data.len() as u64);
    assert_eq!(total_out, expected_data.len() as u64);
    assert_eq!(&output[..expected_data.len()], &expected_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "flate2")]
#[tokio::test]
async fn test_zlib() -> crate::error::Result<()> {
    let file = File::open("testfiles/pg2701.txt.zlib").await?;
    test_async_decompress(zlib::ZlibDecompress::new(), file).await?;
    Ok(())
}

#[cfg(feature = "tokio")]
pub(crate) async fn test_async_decompress<D: Processor + Unpin, R: AsyncRead>(
    decompress: D,
    reader: R,
) -> crate::error::Result<()> {
    use crate::io::AsyncProcessorReader;

    let expected_data = include_bytes!("../testfiles/pg2701.txt");

    let mut output = Vec::new();
    pin!(reader);
    let mut async_reader = AsyncProcessorReader::with_processor(decompress, BufReader::new(reader));
    async_reader.read_to_end(&mut output).await?;
    assert_eq!(output.len(), expected_data.len());
    assert_eq!(&output[..], &expected_data[..]);

    Ok(())
}

#[test]
fn test_plain() -> anyhow::Result<()> {
    test_compress(|| Ok(PlainProcessor::new()), || Ok(PlainProcessor::new()))?;
    test_decompress(
        PlainProcessor::new(),
        include_bytes!("../testfiles/pg2701.txt"),
    )?;
    Ok(())
}

pub struct SmallStepReader<R: Read> {
    inner: R,
    step_size: usize,
}

impl<R: Read> SmallStepReader<R> {
    pub fn new(inner: R, step_size: usize) -> Self {
        Self { inner, step_size }
    }
}

impl<R: Read> Read for SmallStepReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        self.inner.read(&mut buf[..self.step_size.min(buf_len)])
    }
}

pub struct SmallStepWriter<W: Write> {
    writer: W,
    step: usize,
}

impl<W: Write> SmallStepWriter<W> {
    pub fn new(writer: W, step: usize) -> Self {
        Self { writer, step }
    }
}

impl<W: Write> Write for SmallStepWriter<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(&buf[..self.step.min(buf.len())])
    }
}
