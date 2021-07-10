use std::io::{self, prelude::*};
use std::thread;

const READER_BUFFER_SIZE: usize = 1000000;
const READER_BUFFER_COUNT: usize = 2;

enum IoResult {
    Write(Box<dyn io::Write + Send>, Vec<u8>, io::Result<()>),
    Flush(Box<dyn io::Write + Send>, io::Result<()>),
    Read(Box<dyn io::Read + Send>, Vec<u8>, io::Result<usize>),
}

enum IoRequest {
    Write(
        Box<dyn io::Write + Send>,
        Vec<u8>,
        crossbeam_channel::Sender<IoResult>,
    ),
    Flush(
        Box<dyn io::Write + Send>,
        crossbeam_channel::Sender<IoResult>,
    ),
    Read(
        Box<dyn io::Read + Send>,
        Vec<u8>,
        crossbeam_channel::Sender<IoResult>,
    ),
    Finish,
}

/// Run compress, decompress and other I/O tasks in separated threads.
///
/// Example:
/// ```
/// # #[cfg(feature = "thread")] {
/// use autocompress::{iothread::IoThread, open};
/// use std::io::{prelude::*, self};
/// # fn main() -> io::Result<()> {
///
/// let nothread_reader = open("testfiles/plain.txt")?;
/// let thread_pool = IoThread::new(2);
/// let mut threaded_reader = thread_pool.add_reader(nothread_reader)?;
/// let mut buffer = Vec::new();
/// threaded_reader.read_to_end(&mut buffer)?;
/// assert_eq!(buffer, b"ABCDEFG\r\n1234567");
/// # Ok(())
/// # }
/// # }
/// ```
pub struct IoThread {
    thread_pool: Vec<thread::JoinHandle<()>>,
    sender: crossbeam_channel::Sender<IoRequest>,
}

impl IoThread {
    /// Create new I/O thread pool
    pub fn new(threads_num: usize) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(threads_num * 2);
        let thread_pool = (0..threads_num)
            .map(|i| {
                let receiver = receiver.clone();
                thread::spawn(move || worker_thread(i, receiver))
            })
            .collect();
        IoThread {
            thread_pool,
            sender,
        }
    }

    /// Register new writer and create threaded writer.
    pub fn add_writer<W: io::Write + Send + 'static>(&self, writer: W) -> ThreadWriter<W> {
        let (result_sender, result_receiver) = crossbeam_channel::bounded(2);

        ThreadWriter {
            sender: &self.sender,
            result_receiver,
            result_sender,
            buffer: Some(Vec::new()),
            writer: Some(Box::new(writer)),
            closed: false,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Register new writer and create threaded reader.
    pub fn add_reader<R: io::Read + Send + 'static>(
        &self,
        reader: R,
    ) -> io::Result<ThreadReader<R>> {
        self.add_reader_with_capacity(reader, READER_BUFFER_SIZE)
    }

    /// Register new writer and create threaded reader with buffer capacity.
    pub fn add_reader_with_capacity<R: io::Read + Send + 'static>(
        &self,
        reader: R,
        capacity: usize,
    ) -> io::Result<ThreadReader<R>> {
        let (result_sender, result_receiver) = crossbeam_channel::bounded(2);

        self.sender
            .send(IoRequest::Read(
                Box::new(reader),
                vec![0; capacity],
                result_sender.clone(),
            ))
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Send IO read request error"))?;

        let mut processed_buffer = vec![];
        for _ in 1..READER_BUFFER_COUNT {
            processed_buffer.push(vec![0; capacity]);
        }

        Ok(ThreadReader {
            sender: &self.sender,
            result_receiver,
            result_sender,
            buffer: vec![],
            processed_buffer,
            reader: None,
            closed: false,
            eof: false,
            error: None,
            current_point: 0,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl Drop for IoThread {
    fn drop(&mut self) {
        for _ in self.thread_pool.iter() {
            let send_result = self.sender.send(IoRequest::Finish);
            if let Err(e) = send_result {
                eprintln!("Send finish request error: {}", e);
                log::error!("Send finish request error: {}", e);
            }
        }
    }
}

/// Threaded reader
pub struct ThreadReader<'a, R: io::Read + Send> {
    sender: &'a crossbeam_channel::Sender<IoRequest>,
    result_receiver: crossbeam_channel::Receiver<IoResult>,
    result_sender: crossbeam_channel::Sender<IoResult>,
    buffer: Vec<(Vec<u8>, usize)>,
    processed_buffer: Vec<Vec<u8>>,
    reader: Option<Box<dyn Read + Send>>,
    closed: bool,
    eof: bool,
    error: Option<io::ErrorKind>,
    current_point: usize,
    _phantom: std::marker::PhantomData<R>,
}

impl<'a, R: io::Read + Send> ThreadReader<'a, R> {
    #[inline]
    fn dispatch_read_if_required(&mut self) -> io::Result<()> {
        if self.eof {
            return Ok(());
        }
        if !self.processed_buffer.is_empty() {
            if let Some(reader) = self.reader.take() {
                let buffer = self.processed_buffer.remove(0);
                self.sender
                    .send(IoRequest::Read(reader, buffer, self.result_sender.clone()))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "Cannot send I/O read request")
                    })?;
                self.current_point = 0;
            }
        }

        Ok(())
    }

    #[inline]
    fn process_io_result(&mut self, result: IoResult) -> io::Result<()> {
        if let IoResult::Read(reader, buf, result) = result {
            self.reader = Some(reader);
            match result {
                Ok(s) => {
                    if s == 0 {
                        self.eof = true;
                    }
                    self.buffer.push((buf, s));
                }
                Err(e) => {
                    self.error = Some(e.kind());
                    return Err(e);
                }
            }
            Ok(())
        } else {
            unreachable!()
        }
    }

    fn recv_result(&mut self, buffer_required: bool) -> io::Result<()> {
        match self.result_receiver.try_recv() {
            Ok(result) => self.process_io_result(result)?,
            Err(crossbeam_channel::TryRecvError::Empty) => (),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                self.error = Some(io::ErrorKind::Other);
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "I/O result receiver disconnected",
                ));
            }
        }

        while buffer_required && self.buffer.is_empty() && !self.eof {
            match self.result_receiver.recv() {
                Ok(x) => self.process_io_result(x)?,
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        }

        self.dispatch_read_if_required()?;
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.recv_result(true)?;

        // drop reader
        self.reader.take();
        Ok(())
    }
}

impl<'a, R: io::Read + Send> Drop for ThreadReader<'a, R> {
    fn drop(&mut self) {
        self.close().expect("Failed to close reader");
    }
}

impl<'a, R: io::Read + Send> Read for ThreadReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv_result(true)?;
        if self.eof && self.buffer.is_empty() {
            return Ok(0);
        }
        let remain_bytes = self.buffer[0].1 - self.current_point;
        log::debug!(
            "read {} {} {} {}",
            self.buffer[0].1,
            self.current_point,
            remain_bytes,
            buf.len()
        );
        if remain_bytes <= buf.len() {
            buf[0..remain_bytes].copy_from_slice(
                &self.buffer[0].0[self.current_point..(self.current_point + remain_bytes)],
            );
            self.processed_buffer.push(self.buffer.remove(0).0);
            self.current_point = 0;
            Ok(remain_bytes)
        } else {
            buf.copy_from_slice(
                &self.buffer[0].0[self.current_point..(self.current_point + buf.len())],
            );
            self.current_point += buf.len();
            Ok(buf.len())
        }
    }
}

impl<'a, R: io::Read + Send> BufRead for ThreadReader<'a, R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if !self.buffer.is_empty() {
            if self.buffer[0].1 == self.current_point {
                self.processed_buffer.push(self.buffer.remove(0).0);
                self.current_point = 0;
            }
        }
        self.recv_result(true)?;
        Ok(&self.buffer[0].0[self.current_point..self.buffer[0].1])
    }
    fn consume(&mut self, amt: usize) {
        self.current_point += amt;
        if self.current_point > self.buffer[0].1 {
            unreachable!()
        }
    }
}

/// Threaded writer
pub struct ThreadWriter<'a, W: io::Write + Send> {
    sender: &'a crossbeam_channel::Sender<IoRequest>,
    result_receiver: crossbeam_channel::Receiver<IoResult>,
    result_sender: crossbeam_channel::Sender<IoResult>,
    buffer: Option<Vec<u8>>,
    writer: Option<Box<dyn Write + Send>>,
    closed: bool,
    _phantom: std::marker::PhantomData<W>,
}

impl<'a, W: io::Write + Send> ThreadWriter<'a, W> {
    fn recv_result(&mut self, buffer_required: bool, writer_required: bool) -> io::Result<()> {
        while (buffer_required && self.buffer.is_none())
            || (writer_required && self.writer.is_none())
        {
            match self.result_receiver.recv() {
                Ok(IoResult::Flush(writer, result)) => {
                    self.writer = Some(writer);
                    if let Err(e) = result {
                        return Err(e);
                    }
                }
                Ok(IoResult::Write(writer, buf, result)) => {
                    self.writer = Some(writer);
                    self.buffer = Some(buf);
                    if let Err(e) = result {
                        return Err(e);
                    }
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.recv_result(true, true)?;

        // drop writer and buffer
        self.writer.take();
        self.buffer.take();
        Ok(())
    }
}

impl<'a, W: io::Write + Send> Drop for ThreadWriter<'a, W> {
    fn drop(&mut self) {
        self.close().expect("Failed to close writer");
    }
}

impl<'a, W: io::Write + Send> Write for ThreadWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Already closed"));
        }
        self.recv_result(true, true)?;
        let writer = self.writer.take().unwrap();
        let mut buffer = self.buffer.take().unwrap();
        buffer.clear();
        buffer.extend_from_slice(buf);
        self.sender
            .send(IoRequest::Write(writer, buffer, self.result_sender.clone()))
            .map_err(|_e| io::Error::new(io::ErrorKind::Other, "failed to send flush request"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Already closed"));
        }
        self.recv_result(false, true)?;
        let writer = self.writer.take().unwrap();
        self.sender
            .send(IoRequest::Flush(writer, self.result_sender.clone()))
            .map_err(|_e| io::Error::new(io::ErrorKind::Other, "failed to send flush request"))?;
        Ok(())
    }
}

fn worker_thread(thread_index: usize, receiver: crossbeam_channel::Receiver<IoRequest>) {
    log::trace!("IO Thread started: {}", thread_index);
    loop {
        match receiver.recv() {
            Ok(request) => match request {
                IoRequest::Finish => {
                    log::trace!("IO Thread finish request: {}", thread_index);
                    break;
                }
                IoRequest::Write(mut writer, data, sender) => {
                    log::debug!("write request: {}", thread_index);
                    let result = writer.write_all(&data);
                    if let Err(e) = sender.send(IoResult::Write(writer, data, result)) {
                        log::debug!("IO Thread Send Error (write): {}", e);
                        // break;
                    }
                }
                IoRequest::Flush(mut writer, sender) => {
                    log::debug!("flush request: {}", thread_index);
                    let result = writer.flush();
                    if let Err(e) = sender.send(IoResult::Flush(writer, result)) {
                        log::debug!("IO Thread Send Error (flush): {}", e);
                        // break;
                    }
                }
                IoRequest::Read(mut reader, mut data, sender) => {
                    log::debug!("read request: {}", thread_index);
                    let result = reader.read(&mut data);
                    if let Err(e) = sender.send(IoResult::Read(reader, data, result)) {
                        log::debug!("IO Thread Send Error (read): {}", e);
                        // break;
                    }
                }
            },
            Err(e) => {
                log::error!("IO Thread Receive error: {}", e);
                break;
            }
        }
    }

    log::trace!("IO Thread finished: {}", thread_index);
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;
    use std::mem::drop;

    #[test]
    fn write_test1() -> io::Result<()> {
        let writer1 = fs::File::create("target/io-thread-1-1.txt")?;
        let writer2 = fs::File::create("target/io-thread-1-2.txt")?;
        let writer3 = fs::File::create("target/io-thread-1-3.txt")?;

        let iothread = IoThread::new(2);
        let mut writer1 = iothread.add_writer(writer1);
        let mut writer2 = iothread.add_writer(writer2);
        let mut writer3 = iothread.add_writer(writer3);

        for i in 0..4 {
            writeln!(writer1, "{} 0123456789", i)?;
            writeln!(writer2, "{} 0123456789", i)?;
            writeln!(writer3, "{} 0123456789", i)?;
        }

        writer1.flush()?;
        writer2.flush()?;
        writer3.flush()?;

        for i in 0..4 {
            writeln!(writer1, "{} 0123456789", i)?;
            writeln!(writer2, "{} 0123456789", i)?;
            writeln!(writer3, "{} 0123456789", i)?;
        }

        drop(writer1);
        drop(writer2);
        drop(writer3);

        let expected = b"0 0123456789\n1 0123456789\n2 0123456789\n3 0123456789\n0 0123456789\n1 0123456789\n2 0123456789\n3 0123456789\n";

        assert_eq!(fs::read("target/io-thread-1-1.txt")?, expected);
        assert_eq!(fs::read("target/io-thread-1-2.txt")?, expected);
        assert_eq!(fs::read("target/io-thread-1-3.txt")?, expected);

        Ok(())
    }

    #[test]
    fn test_read1() -> io::Result<()> {
        //std::env::set_var("RUST_LOG", "debug");
        //pretty_env_logger::init();
        let iothread = IoThread::new(1);
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut reader1 =
            iothread.add_reader_with_capacity(fs::File::open("./testfiles/plain.txt")?, 9)?;
        let mut reader2 =
            iothread.add_reader_with_capacity(fs::File::open("./testfiles/plain.txt")?, 9)?;

        let mut buffer1 = vec![0u8; 9];
        assert_eq!(reader1.read(&mut buffer1)?, 9);
        assert_eq!(buffer1, expected_bytes[0..9]);
        assert_eq!(reader1.read(&mut buffer1)?, 7);
        assert_eq!(buffer1[0..7], expected_bytes[9..16]);

        let mut buffer2 = Vec::new();
        reader2.read_to_end(&mut buffer2)?;
        assert_eq!(buffer2, expected_bytes);

        Ok(())
    }

    #[test]
    fn test_read2() -> io::Result<()> {
        //std::env::set_var("RUST_LOG", "debug");
        //pretty_env_logger::init();
        let iothread = IoThread::new(1);
        let expected_bytes = include_bytes!("../testfiles/plain.txt");
        let mut reader1 =
            iothread.add_reader_with_capacity(fs::File::open("./testfiles/plain.txt")?, 3)?;
        let mut reader2 =
            iothread.add_reader_with_capacity(fs::File::open("./testfiles/plain.txt")?, 3)?;

        let mut buffer1 = vec![0u8; 9];
        assert_eq!(reader1.read(&mut buffer1)?, 3);
        assert_eq!(buffer1[0..3], expected_bytes[0..3]);
        assert_eq!(reader1.read(&mut buffer1)?, 3);
        assert_eq!(buffer1[0..3], expected_bytes[3..6]);

        let mut buffer2 = Vec::new();
        reader2.read_to_end(&mut buffer2)?;
        assert_eq!(buffer2, expected_bytes);

        Ok(())
    }
}
