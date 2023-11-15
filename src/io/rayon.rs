use crossbeam_channel::{
    Receiver, RecvError, RecvTimeoutError, SendError, SendTimeoutError, Sender, TryRecvError,
    TrySendError,
};
use std::{io::prelude::*, io::Result, ops::Deref};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Buf {
    data: Vec<u8>,
    start: usize,
    len: usize,
    eof: bool,
    flush: bool,
}

impl Buf {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            start: 0,
            len: 0,
            eof: false,
            flush: false,
        }
    }

    pub fn len(&self) -> usize {
        self.len - self.start
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.len = 0;
        self.start = 0;
        self.eof = false;
        self.flush = false;
    }

    pub fn unfilled_buf(&mut self) -> &mut [u8] {
        &mut self.data[self.len..]
    }

    pub fn filled_buf(&self) -> &[u8] {
        &self.data[self.start..self.len]
    }

    pub fn advance(&mut self, len: usize) {
        self.len += len;
        if self.len > self.data.len() {
            self.len = self.data.len();
        }
    }

    pub fn consume(&mut self, len: usize) {
        self.start += len;
        if self.start > self.len {
            self.start = self.len;
        }
    }
}

impl Deref for Buf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.filled_buf()
    }
}

const TIMEOUT_DURATION: std::time::Duration = std::time::Duration::from_millis(10);

fn receive_or_yield<R>(receiver: &Receiver<R>) -> std::result::Result<R, RecvError> {
    loop {
        match receiver.try_recv() {
            Ok(t) => return Ok(t),
            Err(TryRecvError::Empty) => match rayon::yield_now() {
                None => return receiver.recv(),
                Some(rayon::Yield::Executed) => continue,
                Some(rayon::Yield::Idle) => match receiver.recv_timeout(TIMEOUT_DURATION) {
                    Ok(t) => return Ok(t),
                    Err(RecvTimeoutError::Timeout) => {
                        //dbg!("receive idle");
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return Err(RecvError),
                },
            },
            Err(TryRecvError::Disconnected) => return Err(RecvError),
        }
    }
}

fn send_or_yield<R>(sender: &Sender<R>, data: R) -> std::result::Result<(), SendError<R>> {
    match sender.try_send(data) {
        Ok(_) => return Ok(()),
        Err(TrySendError::Full(data)) => match rayon::yield_now() {
            None => return sender.send(data),
            Some(rayon::Yield::Executed) => return send_or_yield(sender, data),
            Some(rayon::Yield::Idle) => match sender.send_timeout(data, TIMEOUT_DURATION) {
                Ok(t) => return Ok(t),
                Err(SendTimeoutError::Timeout(data)) => {
                    //dbg!("send idle");
                    return send_or_yield(sender, data);
                }
                Err(SendTimeoutError::Disconnected(data)) => return Err(SendError(data)),
            },
        },
        Err(TrySendError::Disconnected(data)) => return Err(SendError(data)),
    }
}

pub trait ThreadBuilder {
    fn new_thread<F: FnOnce() -> () + Send + 'static>(&self, f: F);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RayonThreadBuilder;

impl ThreadBuilder for RayonThreadBuilder {
    fn new_thread<F: FnOnce() -> () + Send + 'static>(&self, f: F) {
        rayon::spawn(move || f())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SystemThreadBuilder;

impl ThreadBuilder for SystemThreadBuilder {
    fn new_thread<F: FnOnce() -> () + Send + 'static>(&self, f: F) {
        std::thread::spawn(move || f());
    }
}

fn read_work<R: Read>(mut reader: R, buf: &mut Buf) -> Result<()> {
    let read_bytes = reader.read(buf.unfilled_buf())?;
    buf.advance(read_bytes);
    Ok(())
}

fn read_thread<R: Read>(mut reader: R, mut buf: Buf, sender: Sender<(R, Buf, Result<()>)>) {
    let result = read_work(&mut reader, &mut buf);
    send_or_yield(&sender, (reader, buf, result)).expect("Failed to send read buffer");
}

/// Off load read operation to another thread.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::io::BufReader;
/// # use std::fs::File;
/// use autocompress::io::RayonReader;
/// use autocompress::zstd::ZstdDecompressReader;
///
/// # fn main() -> anyhow::Result<()> {
/// let buf_reader = BufReader::new(File::open("testfiles/sqlite3.c.zst")?);
/// let zstd_reader = ZstdDecompressReader::new(buf_reader);
/// let mut rayon_reader = RayonReader::new(zstd_reader);
/// let mut buf = Vec::new();
/// rayon_reader.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub struct RayonReader<R: Read + Send, TB: ThreadBuilder> {
    receiver: Receiver<(R, Buf, Result<()>)>,
    sender: Sender<(R, Buf, Result<()>)>,
    thread_builder: TB,
    buf: Buf,
    eof: bool,
}

const DEFAULT_RAYON_READER_BUFFER_SIZE: usize = 10_000_000;

impl<R: Read + Send + 'static> RayonReader<R, RayonThreadBuilder> {
    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        Self::with_thread_builder_and_capacity(reader, RayonThreadBuilder, capacity)
    }

    pub fn new(reader: R) -> Self {
        Self::with_capacity(reader, DEFAULT_RAYON_READER_BUFFER_SIZE)
    }
}

impl<R: Read + Send + 'static, TB: ThreadBuilder> RayonReader<R, TB> {
    pub fn with_thread_builder_and_capacity(
        reader: R,
        thread_builder: TB,
        capacity: usize,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(1);

        {
            let sender = sender.clone();
            thread_builder.new_thread(move || read_thread(reader, Buf::new(capacity), sender));
        }

        RayonReader {
            receiver,
            sender,
            thread_builder,
            buf: Buf::new(capacity),
            eof: false,
        }
    }

    fn fill_buffer(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            let (reader, mut new_buf, result) =
                receive_or_yield(&self.receiver).expect("Failed to receive read buffer");
            match result {
                Ok(_) => {
                    if new_buf.is_empty() {
                        self.eof = true;
                        return Ok(());
                    }
                    std::mem::swap(&mut self.buf, &mut new_buf);
                    new_buf.clear();

                    let sender = self.sender.clone();
                    self.thread_builder
                        .new_thread(move || read_thread(reader, new_buf, sender));
                }
                Err(e) => {
                    self.eof = true;
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

impl<R: Read + Send + 'static, TB: ThreadBuilder> Read for RayonReader<R, TB> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.eof {
            return Ok(0);
        }
        self.fill_buffer()?;

        let copy_len = std::cmp::min(buf.len(), self.buf.len());
        buf[..copy_len].copy_from_slice(&self.buf[..copy_len]);
        self.buf.consume(copy_len);

        Ok(copy_len)
    }
}

impl<R: Read + Send + 'static, TB: ThreadBuilder> BufRead for RayonReader<R, TB> {
    fn consume(&mut self, amt: usize) {
        self.buf.consume(amt);
    }
    fn fill_buf(&mut self) -> Result<&[u8]> {
        self.fill_buffer()?;
        Ok(self.buf.filled_buf())
    }
}

/// Off load write operation to another thread.
///
/// If you use this writer with compressor, this writer will run compression in another thread.
/// This writer cannot parallelize compression process.
///
/// ## Example
/// ```
/// # use std::io::prelude::*;
/// # use std::fs::File;
/// use autocompress::io::RayonWriter;
/// use autocompress::zstd::ZstdCompressWriter;
///
/// # fn main() -> anyhow::Result<()> {
/// let file_writer = File::create("target/rayon-doc-write.zst")?;
/// let zstd_writer = ZstdCompressWriter::new(file_writer);
/// let mut rayon_writer = RayonWriter::new(zstd_writer);
/// rayon_writer.write_all(&b"Hello, world\n"[..])?;
/// # Ok(())
/// # }
/// ```
pub struct RayonWriter<W: Write + Send + 'static, TB: ThreadBuilder> {
    sender: Sender<(W, Buf, Result<()>)>,
    receiver: Receiver<(W, Buf, Result<()>)>,
    buf: Buf,
    waiting: Option<(W, Buf)>,
    thread_builder: TB,
}

impl<W: Write + Send + 'static> RayonWriter<W, RayonThreadBuilder> {
    pub fn with_capacity(writer: W, capacity: usize) -> Self {
        Self::with_thread_builder_and_capacity(writer, RayonThreadBuilder, capacity)
    }

    pub fn new(writer: W) -> Self {
        Self::with_capacity(writer, DEFAULT_RAYON_READER_BUFFER_SIZE)
    }
}

impl<W: Write + Send + 'static, TB: ThreadBuilder> RayonWriter<W, TB> {
    pub fn with_thread_builder_and_capacity(
        writer: W,
        thread_builder: TB,
        capacity: usize,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(1);

        RayonWriter {
            sender,
            receiver,
            buf: Buf::new(capacity),
            waiting: Some((writer, Buf::new(capacity))),
            thread_builder,
        }
    }

    fn wait_buffer(&mut self) -> Result<()> {
        if self.waiting.is_some() {
            return Ok(());
        }
        let (writer, mut buf, result) =
            receive_or_yield(&self.receiver).expect("Failed to receive write buffer");
        buf.clear();
        self.waiting.replace((writer, buf));
        result
    }

    fn dispatch_write(&mut self, flush: bool) -> Result<()> {
        //eprintln!("dispatch write");
        self.wait_buffer()?;
        let (mut writer, mut new_buf) = self.waiting.take().unwrap();
        std::mem::swap(&mut self.buf, &mut new_buf);
        let sender = self.sender.clone();
        self.thread_builder.new_thread(move || {
            //eprintln!("write thread");
            let mut result = writer.write_all(&new_buf);
            if flush && result.is_ok() {
                result = writer.flush();
            }
            send_or_yield(&sender, (writer, new_buf, result)).expect("Failed to send write buffer");
        });
        Ok(())
    }
}

impl<W: Write + Send + 'static, TB: ThreadBuilder> Drop for RayonWriter<W, TB> {
    fn drop(&mut self) {
        if !self.buf.is_empty() {
            self.flush().expect("Failed to flush");
        }
        if self.waiting.is_none() {
            self.wait_buffer().expect("Failed to wait buffer");
        }
        if let Some(v) = self.waiting.as_mut() {
            v.0.flush().expect("Failed to flush");
        }
    }
}

impl<W: Write + Send + 'static, TB: ThreadBuilder> Write for RayonWriter<W, TB> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.buf.len() == self.buf.capacity() {
            self.dispatch_write(false)?;
        }
        let copy_len = buf.len().min(self.buf.unfilled_buf().len());
        self.buf.unfilled_buf()[..copy_len].copy_from_slice(&buf[..copy_len]);
        self.buf.advance(copy_len);
        Ok(copy_len)
    }

    fn flush(&mut self) -> Result<()> {
        self.dispatch_write(true)?;
        self.wait_buffer()?;
        if let Some(v) = self.waiting.as_mut() {
            v.0.flush().expect("Failed to flush");
        }
        //eprintln!("flush {}", self.waiting.is_some());
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn test_rayon_reader() -> anyhow::Result<()> {
        let expected_data = include_bytes!("../../testfiles/sqlite3.c");
        let (send, recv) = crossbeam_channel::bounded(100);

        const N: usize = 100;

        for i in 0..N {
            let send = send.clone();
            rayon::spawn(move || {
                eprintln!("start {}", i);

                let mut read_buffer = vec![];
                // write test
                let mut reader = RayonReader::with_thread_builder_and_capacity(
                    std::fs::File::open("testfiles/sqlite3.c").unwrap(),
                    RayonThreadBuilder,
                    101,
                );
                reader.read_to_end(&mut read_buffer).unwrap();
                std::mem::drop(reader);
                assert_eq!(expected_data.len(), read_buffer.len());
                assert_eq!(&expected_data[..], &read_buffer[..]);

                send.send(i).unwrap();
            });
        }

        eprintln!("waiting");

        for x in 0..N {
            let i = recv.recv().unwrap();
            eprintln!("Ok {} {x}", i);
        }

        Ok(())
    }

    #[test]
    pub fn test_rayon_writer() -> anyhow::Result<()> {
        let expected_data = include_bytes!("../../testfiles/sqlite3.c");
        let (send, recv) = crossbeam_channel::bounded(100);

        const N: usize = 100;

        for i in 0..N {
            let send = send.clone();
            rayon::spawn(move || {
                //eprintln!("start {}", i);

                let path = format!("target/test_rayon_writer_{}.c", i);
                let writer_file = std::fs::File::create(&path).unwrap();
                // write test
                let mut writer = RayonWriter::with_thread_builder_and_capacity(
                    writer_file,
                    RayonThreadBuilder,
                    101,
                );
                writer.write_all(&expected_data[..]).unwrap();
                std::mem::drop(writer);

                std::thread::sleep(std::time::Duration::from_millis(10));
                let read_buffer = std::fs::read(path).unwrap();
                assert_eq!(expected_data.len(), read_buffer.len());
                assert_eq!(&expected_data[..], &read_buffer[..]);

                send.send(i).unwrap();
            });
        }

        //eprintln!("waiting");

        for x in 0..N {
            let i = recv.recv().unwrap();
            eprintln!("Ok {} {x}", i);
        }

        Ok(())
    }
}
