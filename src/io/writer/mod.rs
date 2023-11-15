use std::io::{Result, Write};
#[cfg(feature = "tokio")]
use std::{future::Future, task::Poll};
#[cfg(feature = "tokio")]
use tokio::{io::AsyncWrite, pin, sync::Mutex};

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
    writer: W,
    buffer: Vec<u8>,
}

impl<P: Processor + Default, W: Write> ProcessorWriter<P, W> {
    /// Create new ProcessorWriter with default buffer size and default configuration of processor.
    ///
    /// See example of [`ProcessorWriter`] to learn usage.
    pub fn new(writer: W) -> Self {
        Self {
            processor: P::default(),
            writer,
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
            writer,
            buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
        }
    }

    /// Create new ProcessorWriter with a internal buffer size and tuned processor.
    pub fn with_buffer_size(processor: P, writer: W, buffer_size: usize) -> Self {
        Self {
            processor,
            writer,
            buffer: vec![0u8; buffer_size],
        }
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
                .write_all(&self.buffer[..TryInto::<usize>::try_into(processed_out).unwrap()])?;
            write_size += TryInto::<usize>::try_into(processed_in).unwrap();
            //dbg!("wrote data");

            if processed_in == 0 && processed_out == 0 {
                unreachable!("No progress {:?}", status)
            }

            match status {
                Status::MemNeeded => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "More memory is needed",
                    ))
                }
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
                self.writer.write_all(
                    &self.buffer[..TryInto::<usize>::try_into(processed_out).unwrap()],
                )?;
                match status {
                    Status::MemNeeded => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "More memory is needed",
                        ))
                    }
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
    writer: W,
    buffer: Vec<u8>,
    unwritten_buffer_size: usize,
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
    inner: Mutex<Option<AsyncProcessorWriterInner<P, W>>>,
    is_flushed: bool,
}

#[cfg(feature = "tokio")]
impl<P: Processor + Default, W: AsyncWrite> AsyncProcessorWriter<P, W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Mutex::new(Some(AsyncProcessorWriterInner {
                processor: P::default(),
                writer,
                buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
                unwritten_buffer_size: 0,
            })),
            is_flushed: false,
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
            inner: Mutex::new(Some(AsyncProcessorWriterInner {
                processor,
                writer,
                buffer: vec![0u8; PROCESSOR_WRITER_DEFAULT_BUFFER],
                unwritten_buffer_size: 0,
            })),
            is_flushed: false,
        }
    }

    pub fn with_buffer_size(processor: P, writer: W, buffer_size: usize) -> Self {
        Self {
            inner: Mutex::new(Some(AsyncProcessorWriterInner {
                processor,
                writer,
                buffer: vec![0u8; buffer_size],
                unwritten_buffer_size: 0,
            })),
            is_flushed: false,
        }
    }
}

#[cfg(feature = "tokio")]
impl<P: Processor, W: AsyncWrite> Drop for AsyncProcessorWriter<P, W> {
    fn drop(&mut self) {
        if !self.is_flushed {
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
        let writer = &mut inner.writer;
        pin!(writer);

        match writer.poll_write(cx, &inner.buffer[wrote_data..inner.unwritten_buffer_size])? {
            Poll::Ready(written_size) => {
                wrote_data += written_size;
            }
            Poll::Pending => {
                inner
                    .buffer
                    .copy_within(wrote_data..inner.unwritten_buffer_size, 0);
                inner.unwritten_buffer_size -= wrote_data;
                return Poll::Pending;
            }
        }
    }
    inner.unwritten_buffer_size = 0;

    Poll::Ready(Ok(()))
}

#[cfg(feature = "tokio")]
impl<P: Processor, W: AsyncWrite + Unpin> AsyncWrite for AsyncProcessorWriter<P, W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        //dbg!("poll_write", buf.len());
        self.is_flushed = false;
        let this = self.as_mut();
        let inner_mutex = this.inner.lock();
        pin!(inner_mutex);
        let mut inner_option = match inner_mutex.poll(cx) {
            Poll::Ready(inner_option) => inner_option,
            Poll::Pending => return Poll::Pending,
        };
        let mut inner = inner_option.take().expect("No inner data");

        match write_buffer(&mut inner, cx)? {
            Poll::Ready(()) => {}
            Poll::Pending => {
                inner_option.replace(inner);
                return Poll::Pending;
            }
        }

        let mut input_buf = buf;
        let mut output_buf = &mut inner.buffer[..];
        let mut write_size = 0;
        loop {
            let original_total_in = inner.processor.total_in();
            let original_total_out = inner.processor.total_out();
            let status = inner
                .processor
                .process(input_buf, output_buf, Flush::None)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let processed_in = inner.processor.total_in() - original_total_in;
            let processed_out = inner.processor.total_out() - original_total_out;
            input_buf = &input_buf[TryInto::<usize>::try_into(processed_in).unwrap()..];
            output_buf = &mut output_buf[TryInto::<usize>::try_into(processed_out).unwrap()..];
            write_size += TryInto::<usize>::try_into(processed_in).unwrap();
            inner.unwritten_buffer_size += TryInto::<usize>::try_into(processed_out).unwrap();

            if processed_in == 0 && processed_out == 0 {
                unreachable!("No progress {:?}", status)
            }

            match status {
                Status::MemNeeded => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "More memory is needed",
                    )))
                }
                Status::StreamEnd => {
                    inner.processor.reset();
                }
                Status::Ok => {}
            }
            if input_buf.is_empty() || output_buf.is_empty() {
                match write_buffer(&mut inner, cx)? {
                    Poll::Ready(()) => {}
                    Poll::Pending => {
                        inner_option.replace(inner);
                        return Poll::Pending;
                    }
                }

                inner_option.replace(inner);
                return Poll::Ready(Ok(write_size));
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        //dbg!("poll_flush");
        self.is_flushed = true;
        let this = self.as_mut();
        let x = this.inner.lock();
        pin!(x);
        let mut inner_option = match x.poll(cx) {
            Poll::Ready(inner_option) => inner_option,
            Poll::Pending => return Poll::Pending,
        };
        let mut inner = inner_option.take().expect("No inner data");
        if inner.processor.total_in() == 0 {
            return Poll::Ready(Ok(()));
        }

        match write_buffer(&mut inner, cx)? {
            Poll::Ready(()) => {}
            Poll::Pending => {
                inner_option.replace(inner);
                return Poll::Pending;
            }
        }

        let mut output_buf = &mut inner.buffer[..];
        loop {
            let original_total_in = inner.processor.total_in();
            let original_total_out = inner.processor.total_out();
            let status = inner
                .processor
                .process(&[], output_buf, Flush::Finish)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let processed_in = inner.processor.total_in() - original_total_in;
            let processed_out = inner.processor.total_out() - original_total_out;
            output_buf = &mut output_buf[TryInto::<usize>::try_into(processed_out).unwrap()..];
            inner.unwritten_buffer_size += TryInto::<usize>::try_into(processed_out).unwrap();

            if processed_in == 0 && processed_out == 0 {
                unreachable!("No progress {:?}", status)
            }

            match status {
                Status::MemNeeded => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "More memory is needed",
                    )))
                }
                Status::StreamEnd => {
                    inner.processor.reset();
                }
                Status::Ok => {}
            }
            if output_buf.is_empty() || status == Status::StreamEnd {
                match write_buffer(&mut inner, cx)? {
                    Poll::Ready(()) => {}
                    Poll::Pending => {
                        inner_option.replace(inner);
                        return Poll::Pending;
                    }
                }

                inner_option.replace(inner);
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
