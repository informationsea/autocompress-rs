use super::*;
use crate::Processor;
use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::{channel, Receiver, Sender};

const COMPRESSED_BUFFER_EXTEND_SIZE: usize = 1024;

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

const NUMBER_OF_BUFFERS: usize = 20;

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
}

impl<W, P> ParallelCompressWriter<W, P>
where
    W: Write + Send + 'static,
    P: Processor + Send + 'static,
{
    pub fn new<G: Fn() -> P>(writer: W, processor_generator: G) -> Self {
        Self::with_buffer_size(
            writer,
            processor_generator,
            DEFAULT_RAYON_READER_BUFFER_SIZE,
        )
    }

    pub fn with_buffer_size<G: Fn() -> P>(
        writer: W,
        processor_generator: G,
        buffer_size: usize,
    ) -> Self {
        let (compress_result_sender, compress_result_receiver) = channel();
        let (write_result_sender, write_result_receiver) = channel();

        Self {
            writer: Some(writer),
            current_buffer: OrderedBuf::new(processor_generator(), buffer_size),
            next_buffer: (0..NUMBER_OF_BUFFERS)
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
        }
    }

    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.expect("writer should not be None"))
    }

    fn dispatch_write_non_block(&mut self) -> Result<()> {
        self.receive_compress_result(false)?;
        if !self
            .compress_result_buffer
            .contains_key(&self.next_write_index)
        {
            return Ok(());
        }

        if self.writer.is_some() {
            self.dispatch_write()
        } else {
            match self.write_result_receiver.try_recv() {
                Ok(result) => match result {
                    Ok((writer, mut buf)) => {
                        self.writer = Some(writer);
                        buf.clear();
                        self.next_buffer.push(buf);
                        self.dispatch_write()
                    }
                    Err((writer, err)) => {
                        self.writer = Some(writer);
                        return Err(err);
                    }
                },
                Err(TryRecvError::Empty) => Ok(()),
                Err(TryRecvError::Disconnected) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "write result sender is disconnected",
                )),
            }
        }
    }

    fn dispatch_write(&mut self) -> Result<()> {
        //dbg!("dispatch_write");
        if self.writer.is_none() {
            match receive_or_yield(&self.write_result_receiver).map_err(recv_error_to_io_error)? {
                Ok((writer, mut buf)) => {
                    self.writer = Some(writer);
                    buf.clear();
                    self.next_buffer.push(buf);
                }
                Err((writer, err)) => {
                    self.writer = Some(writer);
                    return Err(err);
                }
            }
        }

        if let Some(mut writer) = self.writer.take() {
            while !self
                .compress_result_buffer
                .contains_key(&self.next_write_index)
            {
                let buf = receive_or_yield(&self.compress_result_receiver)
                    .map_err(recv_error_to_io_error)?;
                let buf = match buf {
                    Ok(buf) => buf,
                    Err((buf, err)) => {
                        self.is_error = true;
                        self.next_buffer.push(buf);
                        return Err(err.into());
                    }
                };
                self.compress_result_buffer.insert(buf.index, buf);
            }
            let buf = self
                .compress_result_buffer
                .remove(&self.next_write_index)
                .expect("compress result buffer should contain next write index");
            self.next_write_index += 1;
            let sender = self.write_result_sender.clone();
            rayon::spawn_fifo(move || {
                //dbg!("spawn write", buf.index, buf.compressed.len());
                //eprintln!("spawn write {}", buf.index);
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
        } else {
            unreachable!("writer should not be None");
        }

        Ok(())
    }

    fn receive_compress_result(&mut self, block: bool) -> Result<()> {
        //dbg!("receive_compress_result");
        if block {
            let buf = match receive_or_yield(&self.compress_result_receiver)
                .map_err(recv_error_to_io_error)?
            {
                Ok(buf) => buf,
                Err((buf, err)) => {
                    self.is_error = true;
                    self.next_buffer.push(buf);
                    return Err(err.into());
                }
            };
            self.compress_result_buffer.insert(buf.index, buf);
        }
        while let Ok(buf) = self.compress_result_receiver.try_recv() {
            let buf = match buf {
                Ok(buf) => buf,
                Err((buf, err)) => {
                    self.is_error = true;
                    self.next_buffer.push(buf);
                    return Err(err.into());
                }
            };
            self.compress_result_buffer.insert(buf.index, buf);
        }
        Ok(())
    }

    fn dispatch_compress(&mut self) -> Result<()> {
        while self.next_buffer.is_empty() {
            self.receive_compress_result(true)?;
            self.dispatch_write()?;
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
        self.receive_compress_result(false)?;
        self.dispatch_write_non_block()?;
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
        if self.current_buffer.original.len() > 0 {
            self.dispatch_compress()?;
        }
        while self.next_compress_index != self.next_write_index {
            while !self
                .compress_result_buffer
                .contains_key(&self.next_write_index)
            {
                self.receive_compress_result(true)?;
            }
            self.dispatch_write()?;
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
        Ok(())
    }
}
