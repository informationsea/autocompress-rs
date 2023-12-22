use super::*;
#[cfg(feature = "flate2")]
use crate::gzip::GzipCompress;
#[cfg(feature = "xz")]
use crate::xz::XzCompress;
#[cfg(feature = "zstd")]
use crate::zstd::ZstdCompress;
#[cfg(feature = "flate2")]
use flate2::Compression;
use std::io::prelude::*;
#[cfg(feature = "tokio")]
use tokio::io::AsyncWriteExt;

use crate::tests::SmallStepWriter;

#[cfg(feature = "tokio")]
pub struct AsyncSmallStepWriter<W: AsyncWrite> {
    writer: W,
    step: usize,
    last_mode: u8,
    last_flush_mode: u8,
}

#[cfg(feature = "tokio")]
impl<W: AsyncWrite> AsyncSmallStepWriter<W> {
    pub fn new(writer: W, step: usize) -> Self {
        Self {
            writer,
            step,
            last_mode: 0,
            last_flush_mode: 0,
        }
    }
}

#[cfg(feature = "tokio")]
impl<W: AsyncWrite + Unpin> AsyncWrite for AsyncSmallStepWriter<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        self.last_mode += 1;
        if self.last_mode < 4 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.last_mode = 0;
        let step = self.step;
        let mut this = self.as_mut();
        let writer = &mut this.writer;
        pin!(writer);
        writer.poll_write(cx, &buf[..step.min(buf.len())])
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        self.last_flush_mode += 1;
        if self.last_flush_mode < 4 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.last_mode = 0;
        let mut this = self.as_mut();
        let writer = &mut this.writer;
        pin!(writer);
        writer.poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        self.last_flush_mode += 1;
        if self.last_flush_mode < 4 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.last_mode = 0;
        let mut this = self.as_mut();
        let writer = &mut this.writer;
        pin!(writer);
        writer.poll_shutdown(cx)
    }
}

#[cfg(feature = "flate2")]
#[test]
fn test_writer_gzip() -> anyhow::Result<()> {
    let gzip_compressor = GzipCompress::new(Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = ProcessorWriter::with_processor(gzip_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    std::mem::drop(writer);
    let mut decompress_reader = flate2::bufread::GzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);
    Ok(())
}

#[cfg(feature = "flate2")]
#[test]
fn test_writer_gzip_small_step1() -> anyhow::Result<()> {
    let gzip_compressor = GzipCompress::new(Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = SmallStepWriter::new(
        ProcessorWriter::with_processor(gzip_compressor, &mut write_buffer),
        101,
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    std::mem::drop(writer);
    let mut decompress_reader = flate2::bufread::GzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);
    Ok(())
}

#[cfg(feature = "flate2")]
#[test]
fn test_writer_gzip_small_step2() -> anyhow::Result<()> {
    let gzip_compressor = GzipCompress::new(Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = ProcessorWriter::with_processor(
        gzip_compressor,
        SmallStepWriter::new(&mut write_buffer, 101),
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    std::mem::drop(writer);
    let mut decompress_reader = flate2::bufread::GzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);
    Ok(())
}

#[cfg(feature = "xz")]
#[test]
fn test_writer_xz() -> anyhow::Result<()> {
    let xz_compressor = XzCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = ProcessorWriter::with_processor(xz_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = xz2::bufread::XzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "xz")]
#[test]
fn test_writer_xz_with_flush() -> anyhow::Result<()> {
    let xz_compressor = XzCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = ProcessorWriter::with_processor(xz_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = xz2::bufread::XzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "zstd")]
#[test]
fn test_writer_zstd_with_flush() -> anyhow::Result<()> {
    let xz_compressor = ZstdCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = ProcessorWriter::with_processor(xz_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = zstd::Decoder::new(&write_buffer[..])?;
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "xz")]
#[tokio::test]
async fn async_test_writer_xz() -> anyhow::Result<()> {
    let xz_compressor = XzCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(xz_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = xz2::bufread::XzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "xz")]
#[tokio::test]
async fn async_test_writer_xz_with_shutdown() -> anyhow::Result<()> {
    let xz_compressor = XzCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(xz_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.shutdown().await?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = xz2::bufread::XzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "flate2")]
#[tokio::test]
async fn async_test_writer_gzip() -> anyhow::Result<()> {
    let gzip_compressor = GzipCompress::new(Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(gzip_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = flate2::bufread::MultiGzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "zstd")]
#[tokio::test]
async fn async_test_writer_zstd() -> anyhow::Result<()> {
    let zstd_compressor = ZstdCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(zstd_compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = zstd::Decoder::new(&write_buffer[..])?;
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "zstd")]
#[tokio::test]
async fn async_test_writer_zstd_small_step1() -> anyhow::Result<()> {
    let zstd_compressor = ZstdCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = AsyncSmallStepWriter::new(
        AsyncProcessorWriter::with_processor(zstd_compressor, &mut write_buffer),
        101,
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = zstd::Decoder::new(&write_buffer[..])?;
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "zstd")]
#[tokio::test]
async fn async_test_writer_zstd_small_step2() -> anyhow::Result<()> {
    let zstd_compressor = ZstdCompress::new(6)?;
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(
        zstd_compressor,
        AsyncSmallStepWriter::new(&mut write_buffer, 101),
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = zstd::Decoder::new(&write_buffer[..])?;
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "bgzip")]
#[test]
fn test_writer_bgzip() -> anyhow::Result<()> {
    use crate::bgzip::BgzipCompress;

    let compressor = BgzipCompress::new(::bgzip::Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = ProcessorWriter::with_processor(compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data)?;
    writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = ::flate2::read::MultiGzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "bgzip")]
#[tokio::test]
async fn async_test_writer_bgzip() -> anyhow::Result<()> {
    use crate::bgzip::BgzipCompress;

    let compressor = BgzipCompress::new(::bgzip::Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(compressor, &mut write_buffer);
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.shutdown().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = ::flate2::read::MultiGzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "bgzip")]
#[tokio::test]
async fn async_test_writer_bgzip_small_step1() -> anyhow::Result<()> {
    use crate::bgzip::BgzipCompress;

    let compressor = BgzipCompress::new(::bgzip::Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = AsyncSmallStepWriter::new(
        AsyncProcessorWriter::with_processor(compressor, &mut write_buffer),
        101,
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = ::flate2::read::MultiGzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[cfg(feature = "bgzip")]
#[tokio::test]
async fn async_test_writer_bgzip_small_step2() -> anyhow::Result<()> {
    use crate::bgzip::BgzipCompress;

    let compressor = BgzipCompress::new(::bgzip::Compression::default());
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(
        compressor,
        AsyncSmallStepWriter::new(&mut write_buffer, 101),
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len());
    let mut decompress_reader = ::flate2::read::MultiGzDecoder::new(&write_buffer[..]);
    let mut decompressed_data = Vec::new();
    decompress_reader
        .read_to_end(&mut decompressed_data)
        .expect("Failed to decompress data");
    assert_eq!(original_data.len(), decompressed_data.len());
    assert_eq!(original_data, &decompressed_data[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_test_writer_plain_small_step1() -> anyhow::Result<()> {
    use crate::PlainProcessor;

    let compressor = PlainProcessor::new();
    let mut write_buffer = Vec::new();
    let mut writer = AsyncSmallStepWriter::new(
        AsyncProcessorWriter::with_processor(compressor, &mut write_buffer),
        101,
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    writer.write_all(original_data).await?;
    writer.flush().await?;
    //writer.flush()?;
    std::mem::drop(writer);
    assert_eq!(original_data.len(), write_buffer.len());
    assert_eq!(original_data, &write_buffer[..]);

    Ok(())
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_test_writer_plain_small_step2() -> anyhow::Result<()> {
    use crate::PlainProcessor;

    let compressor = PlainProcessor::new();
    let mut write_buffer = Vec::new();
    let mut writer = AsyncProcessorWriter::with_processor(
        compressor,
        AsyncSmallStepWriter::new(&mut write_buffer, 101),
    );
    let original_data = include_bytes!("../../../testfiles/pg2701.txt");
    dbg!(original_data.len());
    writer.write_all(original_data).await?;
    writer.flush().await?;
    dbg!(
        writer.inner.total_out,
        writer.inner.total_out2,
        writer.inner.total_in
    );

    //writer.flush()?;
    std::mem::drop(writer);
    dbg!(write_buffer.len(), original_data.len());
    assert_eq!(original_data.len(), write_buffer.len());
    assert_eq!(original_data, &write_buffer[..]);

    Ok(())
}
