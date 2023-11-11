#[cfg(feature = "tokio")]
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(feature = "tokio")]
use tokio::{
    io::{AsyncBufRead, AsyncRead, ReadBuf},
    pin,
    sync::Mutex,
};

use crate::{Flush, Processor, Status};
use std::io::{BufRead, Read};

/// This struct that allows reading of data processed by a [`Processor`] from a [`BufRead`].
///
/// ## Type Parameters
/// - `P`: The type of the `Processor` that processes the data.
/// - `R`: The type of the `BufRead` that provides the data.
///
/// ## Example
/// ```ignore
/// # use std::io::prelude::*;
/// # use std::io::BufReader;
/// # use std::fs::File;
/// use autocompress::io::ProcessorReader;
/// use autocompress::zstd::ZstdDecompress;
///
/// # fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.zst")?);
/// let mut zstd_reader = ProcessorReader::<ZstdDecompress, _>::new(buf_reader);
/// let mut buf = Vec::new();
/// zstd_reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub struct ProcessorReader<P: Processor, R: BufRead> {
    processor: P,
    reader: R,
}

impl<P: Processor + Default, R: BufRead> ProcessorReader<P, R> {
    /// Create a new [`ProcessorReader`] from [`BufRead`]
    pub fn new(reader: R) -> Self {
        Self {
            processor: P::default(),
            reader,
        }
    }
}

impl<P: Processor, R: BufRead> ProcessorReader<P, R> {
    /// Create a new [`ProcessorReader`] with specified [`Processor`].
    ///
    /// ## Example
    /// ```ignore
    /// # use std::io::prelude::*;
    /// # use std::io::BufReader;
    /// # use std::fs::File;
    /// use autocompress::io::ProcessorReader;
    /// use autocompress::zstd::ZstdDecompress;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.zst")?);
    /// let zstd_decompress = ZstdDecompress::new()?;
    /// let mut zstd_reader = ProcessorReader::with_processor(zstd_decompress, buf_reader);
    /// let mut buf = Vec::new();
    /// zstd_reader.read_to_end(&mut buf)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_processor(processor: P, reader: R) -> Self {
        Self { processor, reader }
    }
}

impl<P: Processor, R: BufRead> Read for ProcessorReader<P, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let last_total_in = self.processor.total_in();
            let last_total_out = self.processor.total_out();
            let result = self
                .processor
                .process(self.reader.fill_buf()?, buf, Flush::None)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let total_in = self.processor.total_in();
            let total_out = self.processor.total_out();
            self.reader
                .consume((total_in - last_total_in).try_into().unwrap());
            match result {
                Status::StreamEnd => {
                    self.processor.reset();
                }
                Status::MemNeeded => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "MemNeeded"));
                }
                _ => (),
            }

            if total_out - last_total_out > 0 || total_in - last_total_in == 0 {
                return Ok((total_out - last_total_out).try_into().unwrap());
            }
        }
    }
}

#[cfg(feature = "tokio")]
struct AsyncProcessorReaderInner<P: Processor, R: AsyncBufRead + Unpin> {
    processor: P,
    reader: R,
}

/// This struct that allows asynchronous reading of data processed
/// by a [`Processor`] from an [`AsyncBufRead`].
///
/// # Type Parameters
/// - `P`: The type of the [`Processor`] that processes the data.
/// - `R`: The type of the [`AsyncBufRead`] that provides the data. It must also implement [`Unpin`] to ensure it is safe to use in async contexts.
///
/// ## Example
/// ```
/// # use tokio::io::AsyncReadExt;
/// # use tokio::io::BufReader;
/// # use tokio::fs::File;
/// use autocompress::io::AsyncProcessorReader;
/// use autocompress::zstd::ZstdDecompress;
///
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.zst").await?);
/// let mut zstd_reader = AsyncProcessorReader::<ZstdDecompress, _>::new(buf_reader);
/// let mut buf = Vec::new();
/// zstd_reader.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub struct AsyncProcessorReader<P: Processor, R: AsyncBufRead + Unpin> {
    inner: Mutex<Option<AsyncProcessorReaderInner<P, R>>>,
}

#[cfg(feature = "tokio")]
impl<P: Processor + Default, R: AsyncBufRead + Unpin> AsyncProcessorReader<P, R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: Mutex::new(Some(AsyncProcessorReaderInner {
                processor: P::default(),
                reader,
            })),
        }
    }
}

#[cfg(feature = "tokio")]
impl<P: Processor, R: AsyncBufRead + Unpin> AsyncProcessorReader<P, R> {
    pub fn with_processor(processor: P, reader: R) -> Self {
        Self {
            inner: Mutex::new(Some(AsyncProcessorReaderInner { processor, reader })),
        }
    }
}

#[cfg(feature = "tokio")]
impl<P: Processor, R: AsyncBufRead + Unpin> AsyncRead for AsyncProcessorReader<P, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.as_mut();
        let inner_mutex = this.inner.lock();
        pin!(inner_mutex);
        let mut inner_option = match inner_mutex.poll(cx) {
            Poll::Ready(inner_option) => inner_option,
            Poll::Pending => return Poll::Pending,
        };
        let mut inner = inner_option.take().expect("No inner data");

        let mut_reader = &mut inner.reader;
        pin!(mut_reader);
        let reader_buf = match mut_reader.poll_fill_buf(cx)? {
            Poll::Ready(buf) => buf,
            Poll::Pending => {
                inner_option.replace(inner);
                return Poll::Pending;
            }
        };
        if reader_buf.is_empty() {
            // EOF
            inner_option.replace(inner);
            return Poll::Ready(Ok(()));
        }

        let decompress = &mut inner.processor;
        let last_in = decompress.total_in();
        let last_out = decompress.total_out();
        let status = decompress
            .process(reader_buf, buf.initialize_unfilled(), Flush::None)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let processed_in = decompress.total_in() - last_in;
        let processed_out = decompress.total_out() - last_out;

        buf.advance(processed_out as usize);

        let mut_reader = &mut inner.reader;
        pin!(mut_reader);
        mut_reader.consume(processed_in as usize);

        if status == Status::StreamEnd {
            decompress.reset();
        }

        inner_option.replace(inner);

        if processed_out == 0 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        return Poll::Ready(Ok(()));
    }
}

#[cfg(test)]
mod test;
