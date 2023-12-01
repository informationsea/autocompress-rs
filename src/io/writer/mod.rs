#[cfg(feature = "tokio")]
use std::task::Poll;
use std::{
    io::{Result, Write},
    task::ready,
};
#[cfg(feature = "tokio")]
use tokio::{io::AsyncWrite, pin};

use crate::{Flush, Processor, Status};

const PROCESSOR_WRITER_DEFAULT_BUFFER: usize = 1024 * 1024;

/// This struct that allows writing of data processed by a [`Processor`] to a [`Write`].
///
/// # Type Parameters
/// - `P`: The type of the [`Processor`] that processes the data.
/// - `W`: The type of the [`Write`] that the processed data is written to.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::fs::File;
/// use autocompress::io::ProcessorWriter;
/// # #[cfg(feature = "zstd")]
/// use autocompress::zstd::ZstdCompress;
///
/// # fn main() -> anyhow::Result<()> {
/// # #[cfg(feature = "zstd")]
/// let file_writer = File::create("target/doc-io-write.zst")?;
/// # #[cfg(feature = "zstd")]
/// let mut zstd_writer = ProcessorWriter::<ZstdCompress, _>::new(file_writer);
/// # #[cfg(feature = "zstd")]
/// zstd_writer.write_all(&b"Hello, world!"[..])?;
/// # Ok(())
/// # }
/// ```
pub struct ProcessorWriter<P: Processor, W: Write> {
    processor: P,
    writer: Option<W>,
    buffer: Vec<u8>,
}

impl<P: Processor + Default, W: Write> ProcessorWriter<P, W> {
    /// Create new ProcessorWriter with default buffer size and default configuration of processor.
    ///
    /// See example of [`ProcessorWriter`] to learn usage.
    pub fn new(writer: W) -> Self {
        Self {
            processor: P::default(),
            writer: Some(writer),
            buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
        }
    }
}

impl<P: Processor, W: Write> ProcessorWriter<P, W> {
    /// Create new ProcessorWriter with default buffer size and tuned processor.
    ///
    /// ## Example
    /// ```ignore
    /// # use std::io::prelude::*;
    /// # use std::fs::File;
    /// use autocompress::io::ProcessorWriter;
    /// use autocompress::zstd::ZstdCompress;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let file_writer = File::create("target/doc-io-write-with-processor.zst")?;
    /// let zstd_compress = ZstdCompress::new(1)?;
    /// let mut zstd_writer = ProcessorWriter::<ZstdCompress, _>::with_processor(zstd_compress, file_writer);
    /// zstd_writer.write_all(&b"Hello, world!"[..])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_processor(processor: P, writer: W) -> Self {
        Self {
            processor,
            writer: Some(writer),
            buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
        }
    }

    /// Create new ProcessorWriter with a internal buffer size and tuned processor.
    pub fn with_buffer_size(processor: P, writer: W, buffer_size: usize) -> Self {
        Self {
            processor,
            writer: Some(writer),
            buffer: vec![0u8; buffer_size],
        }
    }

    /// Unwraps this ProcessorWriter<P, W>, returning the underlying writer.
    pub fn into_inner_writer(mut self) -> W {
        if self.processor.total_in() != 0 {
            let _ = self.flush().expect("Failed to close ProcessorWriter");
        }
        self.processor.reset();
        self.writer.take().unwrap()
    }

    /// Total number of bytes processed by the processor.
    pub fn total_in(&self) -> u64 {
        self.processor.total_in()
    }

    /// Total number of bytes written to the writer.
    pub fn total_out(&self) -> u64 {
        self.processor.total_out()
    }
}

impl<P: Processor, W: Write> Drop for ProcessorWriter<P, W> {
    fn drop(&mut self) {
        if self.processor.total_in() != 0 {
            let _ = self.flush().expect("Failed to close ProcessorWriter");
        }
    }
}

impl<P: Processor, W: Write> Write for ProcessorWriter<P, W> {
    fn write(&mut self, mut buf: &[u8]) -> Result<usize> {
        let mut write_size = 0;
        loop {
            let original_total_in = self.processor.total_in();
            let original_total_out = self.processor.total_out();
            //dbg!(original_total_in, original_total_out, buf.len());
            let status = self
                .processor
                .process(buf, &mut self.buffer, Flush::None)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let processed_in = self.processor.total_in() - original_total_in;
            let processed_out = self.processor.total_out() - original_total_out;
            // dbg!(
            //     processed_in,
            //     processed_out,
            //     status,
            //     self.processor.total_in(),
            //     self.processor.total_out()
            // );
            buf = &buf[TryInto::<usize>::try_into(processed_in).unwrap()..];
            // dbg!(buf.len());
            //dbg!(processed_out, status);
            self.writer
                .as_mut()
                .unwrap()
                .write_all(&self.buffer[..TryInto::<usize>::try_into(processed_out).unwrap()])?;
            write_size += TryInto::<usize>::try_into(processed_in).unwrap();
            //dbg!("wrote data");

            if processed_in == 0 && processed_out == 0 {
                unreachable!("No progress {:?}", status)
            }

            match status {
                Status::StreamEnd => {
                    self.processor.reset();
                }
                Status::Ok => {}
            }
            if buf.is_empty() {
                return Ok(write_size);
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.processor.total_in() != 0 {
            loop {
                let original_total_out = self.processor.total_out();
                let status = self
                    .processor
                    .process(&[], &mut self.buffer, Flush::Finish)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let processed_out = self.processor.total_out() - original_total_out;
                //dbg!(processed_out, status);
                self.writer.as_mut().unwrap().write_all(
                    &self.buffer[..TryInto::<usize>::try_into(processed_out).unwrap()],
                )?;
                match status {
                    Status::StreamEnd => {
                        self.processor.reset();
                        return Ok(());
                    }
                    Status::Ok => {}
                }
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "tokio")]

struct AsyncProcessorWriterInner<P: Processor, W: AsyncWrite> {
    processor: P,
    writer: Option<W>,
    buffer: Vec<u8>,
    unwritten_buffer_size: usize,
    total_in: u64,
    total_out: u64,
    total_out2: u64,
    is_flushed: bool,
    is_stream_end: bool,
}

#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
/// This struct that allows asynchronous writing of data processed by a [`Processor`] to an [`AsyncWrite`].
///
/// # Type Parameters
/// - `P`: The type of the [`Processor`] that processes the data.
/// - `W`: The type of the [`AsyncWrite`] that the processed data is written to.
///
///
/// ## Example
/// ```
/// # use tokio::fs::File;
/// # use tokio::io::AsyncWriteExt;
/// use autocompress::io::AsyncProcessorWriter;
/// # #[cfg(feature = "zstd")]
/// use autocompress::zstd::ZstdCompress;
///
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// # #[cfg(feature = "zstd")]
/// let file_writer = File::create("target/doc-io-async-write.zst").await?;
/// # #[cfg(feature = "zstd")]
/// let mut zstd_writer = AsyncProcessorWriter::<ZstdCompress, _>::new(file_writer);
/// # #[cfg(feature = "zstd")]
/// zstd_writer.write_all(&b"Hello, world!"[..]).await?;
/// # #[cfg(feature = "zstd")]
/// zstd_writer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct AsyncProcessorWriter<P: Processor, W: AsyncWrite> {
    inner: AsyncProcessorWriterInner<P, W>,
}

#[cfg(feature = "tokio")]
impl<P: Processor + Default, W: AsyncWrite> AsyncProcessorWriter<P, W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: AsyncProcessorWriterInner {
                processor: P::default(),
                writer: Some(writer),
                buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
                unwritten_buffer_size: 0,
                total_in: 0,
                total_out: 0,
                total_out2: 0,
                is_flushed: false,
                is_stream_end: false,
            },
        }
    }
}

#[cfg(feature = "tokio")]
impl<P: Processor, W: AsyncWrite> AsyncProcessorWriter<P, W> {
    /// Create new AsyncProcessorWriter with default buffer size and tuned processor.
    /// /// ## Example
    /// ```
    /// # use tokio::fs::File;
    /// # use tokio::io::AsyncWriteExt;
    /// use autocompress::io::AsyncProcessorWriter;
    /// # #[cfg(feature = "zstd")]
    /// use autocompress::zstd::ZstdCompress;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # #[cfg(feature = "zstd")]
    /// let file_writer = File::create("target/doc-io-async-write-with-processor.zst").await?;
    /// # #[cfg(feature = "zstd")]
    /// let zstd_compress = ZstdCompress::new(1)?;
    /// # #[cfg(feature = "zstd")]
    /// let mut zstd_writer = AsyncProcessorWriter::with_processor(zstd_compress, file_writer);
    /// # #[cfg(feature = "zstd")]
    /// zstd_writer.write_all(&b"Hello, world!"[..]).await?;
    /// # #[cfg(feature = "zstd")]
    /// zstd_writer.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_processor(processor: P, writer: W) -> Self {
        Self {
            inner: AsyncProcessorWriterInner {
                processor,
                writer: Some(writer),
                buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
                unwritten_buffer_size: 0,
                total_in: 0,
                total_out: 0,
                total_out2: 0,
                is_flushed: false,
                is_stream_end: false,
            },
        }
    }

    pub fn with_buffer_size(processor: P, writer: W, buffer_size: usize) -> Self {
        Self {
            inner: AsyncProcessorWriterInner {
                processor,
                writer: Some(writer),
                buffer: vec![0u8; buffer_size],
                unwritten_buffer_size: 0,
                total_in: 0,
                total_out: 0,
                total_out2: 0,
                is_stream_end: false,
                is_flushed: false,
            },
        }
    }

    /// Unwraps this AsyncProcessorWriter<P, W>, returning the underlying writer.
    pub fn into_inner_writer(mut self) -> W {
        if !self.inner.is_flushed {
            panic!("AsyncProcessorWriter is dropped without shutdown")
        }
        self.inner.writer.take().unwrap()
    }
}

#[cfg(feature = "tokio")]
impl<P: Processor, W: AsyncWrite> Drop for AsyncProcessorWriter<P, W> {
    fn drop(&mut self) {
        if !self.inner.is_flushed {
            panic!("AsyncProcessorWriter is dropped without shutdown")
        }
    }
}

#[cfg(feature = "tokio")]
fn write_buffer<P: Processor, W: AsyncWrite + Unpin>(
    inner: &mut AsyncProcessorWriterInner<P, W>,
    cx: &mut std::task::Context<'_>,
) -> Poll<std::result::Result<(), std::io::Error>> {
    let mut wrote_data = 0;
    //dbg!("write_buffer", inner.unwritten_buffer_size);
    while wrote_data < inner.unwritten_buffer_size {
        //dbg!(wrote_data);
        let writer = &mut inner.writer.as_mut().unwrap();
        pin!(writer);
        // dbg!(wrote_data, inner.unwritten_buffer_size, inner.buffer.len());
        match writer.poll_write(cx, &inner.buffer[wrote_data..inner.unwritten_buffer_size])? {
            Poll::Ready(written_size) => {
                //dbg!(written_size, wrote_data,);
                wrote_data += written_size;
                inner.total_out += TryInto::<u64>::try_into(written_size).unwrap();
            }
            Poll::Pending => {
                // dbg!(
                //     "pending",
                //     wrote_data,
                //     inner.unwritten_buffer_size,
                //     inner.buffer.len()
                // );
                inner
                    .buffer
                    .copy_within(wrote_data..inner.unwritten_buffer_size, 0);
                inner.unwritten_buffer_size -= wrote_data;
                if wrote_data == 0 {
                    return Poll::Pending;
                } else {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
    inner.unwritten_buffer_size = 0;

    Poll::Ready(Ok(()))
}

#[cfg(feature = "tokio")]
impl<P: Processor, W: AsyncWrite + Unpin> AsyncWrite for AsyncProcessorWriter<P, W> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        //dbg!("poll_write", buf.len());
        let this = self.get_mut();
        let inner = &mut this.inner;
        if !buf.is_empty() {
            inner.is_flushed = false;
            inner.is_stream_end = false;
        }

        ready!(write_buffer(inner, cx)?);

        let mut input_buf = buf;

        let mut write_size = 0;
        loop {
            let mut output_buf = &mut inner.buffer[inner.unwritten_buffer_size..];
            let original_total_in = inner.processor.total_in();
            let original_total_out = inner.processor.total_out();
            let status = inner
                .processor
                .process(input_buf, output_buf, Flush::None)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let processed_in = inner.processor.total_in() - original_total_in;
            let processed_out = inner.processor.total_out() - original_total_out;
            //dbg!(processed_out, processed_in, status);
            input_buf = &input_buf[TryInto::<usize>::try_into(processed_in).unwrap()..];
            output_buf = &mut output_buf[TryInto::<usize>::try_into(processed_out).unwrap()..];
            write_size += TryInto::<usize>::try_into(processed_in).unwrap();
            inner.total_in += TryInto::<u64>::try_into(processed_in).unwrap();
            inner.total_out2 += TryInto::<u64>::try_into(processed_out).unwrap();
            inner.unwritten_buffer_size += TryInto::<usize>::try_into(processed_out).unwrap();

            if processed_in == 0 && processed_out == 0 {
                unreachable!("No progress {:?}", status)
            }

            match status {
                Status::StreamEnd => {
                    inner.processor.reset();
                }
                Status::Ok => {}
            }
            if input_buf.is_empty() || output_buf.is_empty() {
                match write_buffer(inner, cx)? {
                    Poll::Ready(()) => {
                        return Poll::Ready(Ok(write_size));
                    }
                    Poll::Pending => {
                        if write_size == 0 {
                            return Poll::Pending;
                        } else {
                            return Poll::Ready(Ok(write_size));
                        }
                    }
                }
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        let this = self.get_mut();
        let inner = &mut this.inner;

        loop {
            if !inner.is_stream_end {
                let output_buf = &mut inner.buffer[inner.unwritten_buffer_size..];
                let original_total_out = inner.processor.total_out();
                let status = inner
                    .processor
                    .process(&[], output_buf, Flush::Finish)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let processed_out = inner.processor.total_out() - original_total_out;
                //dbg!(processed_out, status, inner.unwritten_buffer_size);
                inner.total_out2 += TryInto::<u64>::try_into(processed_out).unwrap();
                //output_buf = &mut output_buf[TryInto::<usize>::try_into(processed_out).unwrap()..];
                inner.unwritten_buffer_size += TryInto::<usize>::try_into(processed_out).unwrap();

                match status {
                    Status::StreamEnd => {
                        inner.processor.reset();
                        inner.is_stream_end = true;
                    }
                    Status::Ok => {}
                }
            }
            while inner.unwritten_buffer_size > 0 {
                match write_buffer(inner, cx)? {
                    Poll::Ready(()) => {}
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }

            if inner.is_stream_end && inner.unwritten_buffer_size == 0 {
                let writer = &mut inner.writer.as_mut().unwrap();
                pin!(writer);
                ready!(writer.poll_flush(cx))?;
                inner.is_flushed = true;
                return Poll::Ready(Ok(()));
            }
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        //dbg!("poll_shutdown");
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod test;
