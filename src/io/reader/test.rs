use super::*;

#[cfg(feature = "flate2")]
use crate::gzip::GzipDecompress;
#[cfg(feature = "xz")]
use crate::xz::XzDecompress;
#[cfg(feature = "zstd")]
use crate::zstd::ZstdDecompress;

use anyhow::Context as _;
use std::io::BufReader;

#[cfg(feature = "tokio")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "tokio")]
use tokio::io::BufReader as AsyncBufReader;

struct SmallStepReader<R: Read> {
    inner: R,
    step_size: usize,
}

impl<R: Read> SmallStepReader<R> {
    fn new(inner: R, step_size: usize) -> Self {
        Self { inner, step_size }
    }
}

impl<R: Read> Read for SmallStepReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        self.inner.read(&mut buf[..self.step_size.min(buf_len)])
    }
}

#[cfg(feature = "tokio")]
struct AsyncSmallStepReader<R: AsyncRead> {
    inner: R,
    step_size: usize,
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead> AsyncSmallStepReader<R> {
    fn new(inner: R, step_size: usize) -> Self {
        Self { inner, step_size }
    }
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead + Unpin> AsyncRead for AsyncSmallStepReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let unfilled_buf = buf.initialize_unfilled();
        let unfilled_buf_len = unfilled_buf.len();
        let mut unfilled_read_buf =
            ReadBuf::new(&mut unfilled_buf[..self.step_size.min(unfilled_buf_len)]);
        let mut this = self.as_mut();
        let inner = &mut this.inner;
        pin!(inner);
        match inner.poll_read(cx, &mut unfilled_read_buf) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                let filled_len = unfilled_read_buf.filled().len();
                buf.advance(filled_len);
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(feature = "tokio")]
struct AsyncReadProxy<R: AsyncRead + Unpin> {
    prefix: String,
    inner: R,
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead + Unpin> AsyncReadProxy<R> {
    pub fn new(prefix: &str, reader: R) -> Self {
        Self {
            prefix: prefix.to_string(),
            inner: reader,
        }
    }
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead + Unpin> AsyncRead for AsyncReadProxy<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let inner = &mut this.inner;
        pin!(inner);

        let original_filled = buf.filled().len();
        let original_remaining = buf.remaining();
        let original_capacity = buf.capacity();

        let result = inner.poll_read(cx, buf);

        let new_filled = buf.filled().len();
        let new_remaining = buf.remaining();
        let new_capacity = buf.capacity();

        let prefix = this.prefix.as_str();

        eprintln!("{prefix} read: {result:?} / filled {original_filled} -> {new_filled} / remaining {original_remaining} -> {new_remaining} / capacity {original_capacity} -> {new_capacity}");

        result
    }
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn test_slice_read() -> anyhow::Result<()> {
    let mut file = AsyncReadProxy::new(
        "file_reader",
        tokio::fs::File::open("testfiles/sqlite3.c.xz").await?,
    );
    let original_slice = include_bytes!("../../../testfiles/sqlite3.c.xz");
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    let mut out = Vec::new();
    tokio::io::copy(&mut file, &mut out).await?;
    assert_eq!(original_slice, &buf[..]);
    Ok(())
}

async fn test_all_reader<P: Processor + Unpin, F: Fn() -> P + Clone>(
    processor: F,
    compressed_data: &[u8],
) -> anyhow::Result<()> {
    #[cfg(feature = "tokio")]
    test_async_reader(processor.clone(), compressed_data).await?;
    test_reader(processor, compressed_data)?;
    Ok(())
}

#[cfg(feature = "tokio")]
async fn test_async_reader<P: Processor + Unpin, F: Fn() -> P>(
    processor: F,
    compressed_data: &[u8],
) -> anyhow::Result<()> {
    // strait read
    let mut decompress_reader =
        AsyncProcessorReader::with_processor(processor(), &compressed_data[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .await?;

    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    assert_eq!(decompressed_data.len(), expected_data.len());
    assert_eq!(decompressed_data, expected_data);

    // small step read
    let mut decompress_reader = AsyncSmallStepReader::new(
        AsyncProcessorReader::with_processor(processor(), &compressed_data[..]),
        101,
    );
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .await?;

    assert_eq!(decompressed_data.len(), expected_data.len());
    assert_eq!(decompressed_data, expected_data);

    // small step read
    let mut decompress_reader = AsyncProcessorReader::with_processor(
        processor(),
        AsyncBufReader::new(AsyncSmallStepReader::new(&compressed_data[..], 101)),
    );
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .await?;

    assert_eq!(decompressed_data.len(), expected_data.len());
    assert_eq!(decompressed_data, expected_data);

    Ok(())
}

fn test_reader<P: Processor, F: Fn() -> P>(
    processor: F,
    compressed_data: &[u8],
) -> anyhow::Result<()> {
    // strait read
    let mut decompress_reader = ProcessorReader::with_processor(processor(), &compressed_data[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader.read_to_end(&mut decompressed_data)?;

    let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
    assert_eq!(decompressed_data.len(), expected_data.len());
    assert_eq!(decompressed_data, expected_data);

    // small step read
    let mut decompress_reader = SmallStepReader::new(
        ProcessorReader::with_processor(processor(), &compressed_data[..]),
        101,
    );
    let mut decompressed_data = Vec::new();
    decompress_reader.read_to_end(&mut decompressed_data)?;

    assert_eq!(decompressed_data.len(), expected_data.len());
    assert_eq!(decompressed_data, expected_data);

    // small step read
    let mut decompress_reader = ProcessorReader::with_processor(
        processor(),
        BufReader::new(SmallStepReader::new(&compressed_data[..], 101)),
    );
    let mut decompressed_data = Vec::new();
    decompress_reader.read_to_end(&mut decompressed_data)?;

    assert_eq!(decompressed_data.len(), expected_data.len());
    assert_eq!(decompressed_data, expected_data);

    Ok(())
}

#[cfg(feature = "flate2")]
#[tokio::test]
async fn test_read_gzip() -> anyhow::Result<()> {
    test_all_reader(
        || GzipDecompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.gz"),
    )
    .await
    .context("standard gzip")?;
    test_all_reader(
        || GzipDecompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.multistream.gz"),
    )
    .await
    .context("multistream gzip")?;
    test_all_reader(
        || GzipDecompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.bgzip.gz"),
    )
    .await
    .context("bzgip")?;
    test_all_reader(
        || GzipDecompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.pigz.gz"),
    )
    .await
    .context("pigz")?;
    test_all_reader(
        || GzipDecompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.pipe.gz"),
    )
    .await
    .context("pipe")?;
    Ok(())
}

#[cfg(feature = "xz")]
#[tokio::test]
async fn test_read_xz() -> anyhow::Result<()> {
    test_all_reader(
        || XzDecompress::new(10_000_000).unwrap(),
        include_bytes!("../../../testfiles/sqlite3.c.xz"),
    )
    .await?;
    test_all_reader(
        || XzDecompress::new(10_000_000).unwrap(),
        include_bytes!("../../../testfiles/sqlite3.c.multistream.xz"),
    )
    .await?;
    Ok(())
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn test_read_zstd() -> anyhow::Result<()> {
    test_all_reader(
        || ZstdDecompress::new().unwrap(),
        include_bytes!("../../../testfiles/sqlite3.c.zst"),
    )
    .await?;
    test_all_reader(
        || ZstdDecompress::new().unwrap(),
        include_bytes!("../../../testfiles/sqlite3.c.multistream.zst"),
    )
    .await?;
    Ok(())
}

#[cfg(feature = "bzip2")]
#[tokio::test]
async fn test_read_bzip2() -> anyhow::Result<()> {
    use crate::bzip2::Bzip2Decompress;

    test_all_reader(
        || Bzip2Decompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.bz2"),
    )
    .await?;
    test_all_reader(
        || Bzip2Decompress::new(),
        include_bytes!("../../../testfiles/sqlite3.c.multistream.bz2"),
    )
    .await?;
    Ok(())
}
