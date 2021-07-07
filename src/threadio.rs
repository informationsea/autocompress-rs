use std::io::{self, prelude::*};
use std::thread;

enum IoResult {
    Write(Box<dyn io::Write + Send>, Vec<u8>, io::Result<()>),
    Flush(Box<dyn io::Write + Send>, io::Result<()>),
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
    Finish,
}

/// Thread pool for IO jobs
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

    /// Register new writer
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
}

impl Drop for IoThread {
    fn drop(&mut self) {
        for _ in self.thread_pool.iter() {
            self.sender
                .send(IoRequest::Finish)
                .expect("Failed to send finish request");
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
            }
        }
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
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
    log::debug!("IO Thread started: {}", thread_index);
    loop {
        match receiver.recv() {
            Ok(request) => match request {
                IoRequest::Finish => break,
                IoRequest::Write(mut writer, data, sender) => {
                    log::debug!("write request: {}", thread_index);
                    let result = writer.write_all(&data);
                    if let Err(e) = sender.send(IoResult::Write(writer, data, result)) {
                        log::error!("IO Thread Send Error: {}", e);
                        break;
                    }
                }
                IoRequest::Flush(mut writer, sender) => {
                    log::debug!("flush request: {}", thread_index);
                    let result = writer.flush();
                    if let Err(e) = sender.send(IoResult::Flush(writer, result)) {
                        log::error!("IO Thread Send Error: {}", e);
                        break;
                    }
                }
            },
            Err(e) => {
                log::error!("IO Thread Receive error: {}", e);
                break;
            }
        }
    }

    log::debug!("IO Thread finished: {}", thread_index);
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
}
