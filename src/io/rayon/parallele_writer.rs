use super::*;
use crate::Processor;
use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;

const COMPRESSED_BUFFER_EXTEND_SIZE: usize = 1024;
const NO_TIMEOUT_DURATION: Duration = Duration::from_millis(0);

#[derive(Debug)]
struct OrderedBuf<P: Processor> {
    original: Buf,
    compressed: Buf,
    processor: P,
    index: u64,
}

impl<P: Processor> OrderedBuf<P> {
    pub fn new(processor: P, size: usize) -> Self {
        Self {
            original: Buf::new(size),
            compressed: Buf::new(size + COMPRESSED_BUFFER_EXTEND_SIZE),
            processor,
            index: 0,
        }
    }

    pub fn clear(&mut self) {
        self.original.clear();
        self.compressed.clear();
        self.processor.reset();
        self.index = 0;
    }
}

const NUMBER_OF_BUFFERS: usize = 50;

/// `ParallelCompressWriter` is a struct that handles parallel compression writing.
///
/// # Type Parameters
/// * `W`: This represents the writer where the compressed data will be written. It must implement the `Write` trait, be thread-safe (`Send`), and have a static lifetime (`'static`).
/// * `P`: This represents the processor that will be used for the compression. It must implement the `Processor` trait, be thread-safe (`Send`), and have a static lifetime (`'static`).
pub struct ParallelCompressWriter<W, P>
where
    W: Write + Send + 'static,
    P: Processor + Send + 'static,
{
    writer: Option<W>,
    current_buffer: OrderedBuf<P>,
    next_buffer: Vec<OrderedBuf<P>>,
    buffer_size: usize,
    compress_result_sender:
        Sender<std::result::Result<OrderedBuf<P>, (OrderedBuf<P>, crate::Error)>>,
    compress_result_receiver:
        Receiver<std::result::Result<OrderedBuf<P>, (OrderedBuf<P>, crate::Error)>>,
    compress_result_buffer: HashMap<u64, OrderedBuf<P>>,
    next_compress_index: u64,
    next_write_index: u64,
    write_result_sender: Sender<std::result::Result<(W, OrderedBuf<P>), (W, std::io::Error)>>,
    write_result_receiver: Receiver<std::result::Result<(W, OrderedBuf<P>), (W, std::io::Error)>>,
    is_error: bool,
    is_flushed: bool,
}

impl<W, P> ParallelCompressWriter<W, P>
where
    W: Write + Send + 'static,
    P: Processor + Send + 'static,
{
    /// Constructs a new instance of [`ParallelCompressWriter`].
    ///
    /// # Parameters
    /// * `writer`: This is the writer where the compressed data will be written.
    /// * `processor_generator`: This is a function that generates a new instance of the processor `P`.
    ///
    /// # Returns
    /// A new instance of [`ParallelCompressWriter`].
    ///
    /// # Example
    /// ```
    /// # use std::fs::File;
    /// # use std::io::Write;
    /// # use autocompress::io::ParallelCompressWriter;
    /// # use autocompress::gzip::GzipCompress;
    /// # fn main() -> std::io::Result<()> {
    /// let writer = File::create("target/rayon-parallel-writer.txt.gz")?;
    /// let mut parallel_writer = ParallelCompressWriter::new(writer, || GzipCompress::default());
    /// parallel_writer.write_all(&b"Hello, world\n"[..])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<G: Fn() -> P>(writer: W, processor_generator: G) -> Self {
        Self::with_buffer_size(
            writer,
            processor_generator,
            DEFAULT_RAYON_READER_BUFFER_SIZE,
            NUMBER_OF_BUFFERS,
        )
    }

    /// Constructs a new instance of [`ParallelCompressWriter`] with a specified buffer size and buffer count.
    ///
    /// # Parameters
    /// * `writer`: This is the writer where the compressed data will be written.
    /// * `processor_generator`: This is a function that generates a new instance of the processor `P`.
    /// * `buffer_size`: This is the size of the buffers used for compression.
    /// * `buffer_count`: This is the number of buffers that will be used for compression. This is upper bounded by the number of threads in the rayon thread pool.
    ///
    /// # Returns
    /// A new instance of [`ParallelCompressWriter`] with the specified buffer size and buffer count.
    ///
    /// # Example
    /// ```
    /// # use std::fs::File;
    /// # use std::io::Write;
    /// # use autocompress::io::ParallelCompressWriter;
    /// # use autocompress::gzip::GzipCompress;
    /// # fn main() -> std::io::Result<()> {
    /// let writer = File::create("target/rayon-parallel-writer-with-buffer.txt.gz")?;
    /// let mut parallel_writer = ParallelCompressWriter::with_buffer_size(writer, || GzipCompress::default(), 1024 * 1024, 50);
    /// parallel_writer.write_all(&b"Hello, world\n"[..])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_buffer_size<G: Fn() -> P>(
        writer: W,
        processor_generator: G,
        buffer_size: usize,
        buffer_count: usize,
    ) -> Self {
        let (compress_result_sender, compress_result_receiver) = channel();
        let (write_result_sender, write_result_receiver) = channel();

        Self {
            writer: Some(writer),
            current_buffer: OrderedBuf::new(processor_generator(), buffer_size),
            next_buffer: (0..buffer_count)
                .map(|_| OrderedBuf::new(processor_generator(), buffer_size))
                .collect(),
            buffer_size,
            next_compress_index: 0,
            compress_result_receiver,
            compress_result_sender,
            compress_result_buffer: HashMap::new(),
            next_write_index: 0,
            write_result_receiver,
            write_result_sender,
            is_error: false,
            is_flushed: true,
        }
    }

    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.take().expect("writer should not be None"))
    }

    fn dispatch_write_with_writer(&mut self, mut writer: W, buf: OrderedBuf<P>) {
        self.next_write_index += 1;
        let sender = self.write_result_sender.clone();
        // eprintln!(
        //     "spawn write {} / {} / {} / {}",
        //     buf.index,
        //     buf.compressed.len(),
        //     self.next_compress_index,
        //     self.next_write_index
        // );
        rayon::spawn_fifo(move || {
            //dbg!("spawn write", buf.index, buf.compressed.len());

            match writer.write_all(buf.compressed.as_ref()) {
                Ok(()) => {
                    sender
                        .send(Ok((writer, buf)))
                        .expect("write result sender should not be closed (1)");
                }
                Err(e) => {
                    sender
                        .send(Err((writer, e)))
                        .expect("write result sender should not be closed (2)");
                }
            }
        });
    }

    fn receive_compress_or_write_result(
        &mut self,
        wait_buffer: bool,
        wait_writer: bool,
    ) -> Result<()> {
        loop {
            rayon::yield_now();
            if self.next_compress_index != self.next_write_index {
                match self.compress_result_receiver.recv_timeout(
                    if wait_buffer && self.next_buffer.is_empty() && self.writer.is_some() {
                        TIMEOUT_DURATION
                    } else {
                        NO_TIMEOUT_DURATION
                    },
                ) {
                    Ok(result) => match result {
                        Ok(buf) => {
                            self.compress_result_buffer.insert(buf.index, buf);
                        }
                        Err((buf, err)) => {
                            self.is_error = true;
                            self.next_buffer.push(buf);
                            return Err(err.into());
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "compress result sender is disconnected",
                        ))
                    }
                }
            }

            if self.writer.is_none() {
                match self.write_result_receiver.recv_timeout(
                    if wait_writer || (wait_buffer && self.next_buffer.is_empty()) {
                        TIMEOUT_DURATION
                    } else {
                        NO_TIMEOUT_DURATION
                    },
                ) {
                    Ok(result) => match result {
                        Ok((writer, mut buf)) => {
                            self.writer = Some(writer);
                            buf.clear();
                            self.next_buffer.push(buf);
                        }
                        Err((writer, err)) => {
                            self.writer = Some(writer);
                            self.is_error = true;
                            return Err(err);
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "write result sender is disconnected",
                        ))
                    }
                }
            }

            if let Some(buf) = self.compress_result_buffer.remove(&self.next_write_index) {
                if let Some(writer) = self.writer.take() {
                    self.dispatch_write_with_writer(writer, buf);
                } else {
                    self.compress_result_buffer.insert(buf.index, buf);
                }
            }

            if (!wait_buffer || !self.next_buffer.is_empty())
                && (!wait_writer || self.writer.is_some())
            {
                return Ok(());
            }
        }
    }

    fn dispatch_compress(&mut self) -> Result<()> {
        while self.next_buffer.is_empty() {
            self.receive_compress_or_write_result(true, false)?;
        }
        let mut new_buf = self.next_buffer.pop().unwrap();
        std::mem::swap(&mut new_buf, &mut self.current_buffer);
        new_buf.index = self.next_compress_index;
        self.next_compress_index += 1;

        let sender = self.compress_result_sender.clone();

        rayon::spawn_fifo(move || {
            let mut f = || -> std::result::Result<(), crate::Error> {
                //eprintln!("spawn compress {}", new_buf.index);
                //dbg!("spawn compress", new_buf.index, new_buf.original.len());
                new_buf.processor.reset();
                let status = new_buf.processor.process(
                    &new_buf.original,
                    new_buf.compressed.unfilled_buf(),
                    crate::Flush::Finish,
                )?;
                if status != crate::Status::StreamEnd {
                    return Err(crate::Error::CompressError("Not enough buffer".to_string()));
                }
                new_buf
                    .compressed
                    .advance(new_buf.processor.total_out() as usize);
                // dbg!(
                //     "spawn compress done",
                //     new_buf.index,
                //     new_buf.compressed.len()
                // );
                Ok(())
            };
            match f() {
                Ok(()) => sender
                    .send(Ok(new_buf))
                    .expect("Failed to send compress result"),
                Err(e) => sender
                    .send(Err((new_buf, e.into())))
                    .expect("Failed to send compress error"),
            }
        });
        Ok(())
    }
}

impl<W, P> Write for ParallelCompressWriter<W, P>
where
    W: Write + Send + 'static,
    P: Processor + Send + 'static,
{
    fn write(&mut self, mut buf: &[u8]) -> Result<usize> {
        if self.is_error {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "previous error",
            ));
        }
        self.is_flushed = false;
        self.receive_compress_or_write_result(false, false)?;
        let mut wrote_len = 0;
        while !buf.is_empty() {
            let write_len = buf
                .len()
                .min(self.buffer_size - self.current_buffer.original.len());
            wrote_len += write_len;
            self.current_buffer.original.unfilled_buf()[..write_len]
                .copy_from_slice(&buf[..write_len]);
            self.current_buffer.original.advance(write_len);
            buf = &buf[write_len..];

            if self.current_buffer.original.len() == self.buffer_size {
                self.dispatch_compress()?;
            }
        }
        Ok(wrote_len)
    }

    fn flush(&mut self) -> Result<()> {
        if self.is_flushed {
            return Ok(());
        }
        if self.current_buffer.original.len() > 0 {
            self.dispatch_compress()?;
        }
        while self.next_compress_index != self.next_write_index {
            self.receive_compress_or_write_result(false, true)?;
        }
        while self.writer.is_none() {
            match receive_or_yield(&self.write_result_receiver).map_err(recv_error_to_io_error)? {
                Ok((writer, mut buf)) => {
                    self.writer = Some(writer);
                    buf.clear();
                    self.next_buffer.push(buf);
                }
                Err((writer, err)) => {
                    self.writer = Some(writer);
                    self.is_error = true;
                    return Err(err);
                }
            }
        }
        self.writer.as_mut().unwrap().flush()?;
        self.is_flushed = true;
        Ok(())
    }
}

impl<W, P> Drop for ParallelCompressWriter<W, P>
where
    W: Write + Send + 'static,
    P: Processor + Send + 'static,
{
    fn drop(&mut self) {
        if !self.is_flushed {
            self.flush().expect("Failed to flush");
        }
    }
}
