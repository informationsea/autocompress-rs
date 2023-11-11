use crossbeam_channel::{bounded, Receiver, Sender};
use std::{io::prelude::*, io::Result, marker::PhantomData, ops::Deref};

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
pub struct RayonReader<R: Read> {
    _phantom: std::marker::PhantomData<R>,
    current_data: Option<Result<Buf>>,
    receiver: crossbeam_channel::Receiver<Result<Buf>>,
    sender: crossbeam_channel::Sender<Buf>,
    eof: bool,
    eof_received: bool,
}

const DEFAULT_RAYON_READER_BUFFER_SIZE: usize = 1024 * 1024 * 10;

fn reader_thread<R: Read>(mut reader: R, receiver2: Receiver<Buf>, sender1: Sender<Result<Buf>>) {
    let mut buffer = receiver2.recv().expect("Failed to receive buffer 1");
    loop {
        let read_len = match reader.read(buffer.unfilled_buf()) {
            Ok(read_len) => read_len,
            Err(e) => {
                sender1.send(Err(e)).expect("Failed to send buffer (error)");
                break;
            }
        };
        //dbg!(read_len);
        buffer.advance(read_len);
        if read_len == 0 {
            buffer.eof = true;
            sender1.send(Ok(buffer)).expect("Failed to send buffer 2");
            break;
        } else if buffer.len() == buffer.capacity() {
            let next_buffer = receiver2.recv().expect("Failed to receive buffer 3");
            if next_buffer.eof {
                buffer.eof = true;
                sender1.send(Ok(buffer)).expect("Failed to send buffer 6");
                return;
            }
            sender1.send(Ok(buffer)).expect("Failed to send buffer 3");
            buffer = next_buffer;
            buffer.clear();
        }
    }

    loop {
        match receiver2.recv() {
            Ok(buffer) => {
                if buffer.eof {
                    break;
                }
            }
            Err(_) => {
                break;
            }
        }
    }
}

impl<R: Read + Send + 'static> RayonReader<R> {
    pub fn new(reader: R) -> Self {
        Self::with_capacity(reader, DEFAULT_RAYON_READER_BUFFER_SIZE)
    }

    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        let (sender1, receiver1) = bounded(3);
        let (sender2, receiver2) = bounded(3);

        for _ in 0..3 {
            sender2
                .send(Buf::new(capacity))
                .expect("Failed to send buffer 1");
        }

        rayon::spawn_fifo(move || reader_thread(reader, receiver2, sender1));

        Self {
            _phantom: std::marker::PhantomData,
            current_data: None,
            receiver: receiver1,
            sender: sender2,
            eof: false,
            eof_received: false,
        }
    }
}

impl<'scope, R: Read + Send + 'scope> RayonReader<R> {
    pub fn with_scope(reader: R, scope: &rayon::Scope<'scope>) -> Self {
        Self::with_scope_and_capacity(reader, scope, DEFAULT_RAYON_READER_BUFFER_SIZE)
    }

    pub fn with_scope_and_capacity(
        reader: R,
        scope: &rayon::Scope<'scope>,
        capacity: usize,
    ) -> Self {
        let (sender1, receiver1) = bounded(3);
        let (sender2, receiver2) = bounded(3);

        for _ in 0..3 {
            sender2
                .send(Buf::new(capacity))
                .expect("Failed to send buffer 1");
        }

        scope.spawn(move |_| reader_thread(reader, receiver2, sender1));

        Self {
            _phantom: std::marker::PhantomData,
            current_data: None,
            receiver: receiver1,
            sender: sender2,
            eof: false,
            eof_received: false,
        }
    }
}

impl<R: Read> Read for RayonReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.eof {
            return Ok(0);
        }
        if self.current_data.is_none() {
            self.current_data = Some(self.receiver.recv().expect("Failed to receive buffer 5"));
        }
        let current_data = self.current_data.as_mut().unwrap();
        match current_data {
            Ok(current_data) => {
                let copy_len = buf.len().min(current_data.len());
                buf[..copy_len].copy_from_slice(&current_data[..copy_len]);
                current_data.consume(copy_len);
                //dbg!(copy_len);
                if current_data.len() == 0 {
                    if current_data.eof {
                        //dbg!("eof");
                        self.eof = true;
                        self.eof_received = true;
                    }

                    self.sender
                        .send(self.current_data.take().unwrap().unwrap())
                        .expect("Failed to send buffer 4");
                }
                Ok(copy_len)
            }
            Err(_e) => {
                self.eof = true;
                let mut buffer = Buf::new(0);
                buffer.eof = true;
                self.eof_received = true;
                //self.sender.send(buffer).expect("Failed to send buffer 6");
                Err(self.current_data.take().unwrap().unwrap_err())
            }
        }
    }
}

impl<R: Read> Drop for RayonReader<R> {
    fn drop(&mut self) {
        if !self.eof {
            let mut buffer = Buf::new(0);
            buffer.eof = true;
            self.eof = true;
            self.sender.send(buffer).expect("Failed to send buffer 5");
            loop {
                match self.receiver.recv().expect("Failed to receive buffer 6") {
                    Ok(buf) => {
                        if buf.eof {
                            break;
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
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
pub struct RayonWriter<W: Write> {
    _phantom: std::marker::PhantomData<W>,
    current_buf: Option<Buf>,
    receiver: crossbeam_channel::Receiver<Result<Buf>>,
    sender: crossbeam_channel::Sender<Buf>,
    flush_result_receiver: crossbeam_channel::Receiver<Result<()>>,
    eof: bool,
}

fn write_thread<W: Write>(
    mut writer: W,
    sender1: Sender<Result<Buf>>,
    receiver2: Receiver<Buf>,
    flush_sender: Sender<Result<()>>,
) {
    loop {
        dbg!("receive");
        let buffer: Buf = receiver2.recv().expect("Failed to receive buffer 1");
        if buffer.eof {
            flush_sender
                .send(writer.flush())
                .expect("failed to send flush result");
            return;
        }
        dbg!(buffer.len(), buffer.flush, buffer.eof);
        match writer.write_all(buffer.filled_buf()) {
            Ok(_) => {
                if buffer.flush {
                    dbg!("flush start");
                    flush_sender
                        .send(writer.flush())
                        .expect("failed to send flush result");
                    dbg!("flush end");
                }
                dbg!("send buf");
                sender1.send(Ok(buffer)).expect("Failed to send buffer 2");
            }
            Err(e) => {
                sender1.send(Err(e)).expect("Failed to send buffer 2");
            }
        }
    }
}

impl<W: Write + Send + 'static> RayonWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_capacity(writer, DEFAULT_RAYON_READER_BUFFER_SIZE)
    }

    pub fn with_capacity(writer: W, capacity: usize) -> Self {
        let (sender1, receiver1) = bounded(3);
        let (sender2, receiver2) = bounded(3);
        let (flush_sender, flush_receiver) = bounded(0);

        for _ in 0..2 {
            sender1
                .send(Ok(Buf::new(capacity)))
                .expect("Failed to send buffer 1");
        }

        rayon::spawn_fifo(move || write_thread(writer, sender1, receiver2, flush_sender));

        Self {
            _phantom: PhantomData,
            current_buf: Some(Buf::new(capacity)),
            receiver: receiver1,
            sender: sender2,
            flush_result_receiver: flush_receiver,
            eof: false,
        }
    }
}

impl<'scope, W: Write + Send + 'scope> RayonWriter<W> {
    pub fn with_scope(writer: W, scope: &rayon::Scope<'scope>) -> Self {
        Self::with_scope_and_capacity(writer, scope, DEFAULT_RAYON_READER_BUFFER_SIZE)
    }

    pub fn with_scope_and_capacity(
        writer: W,
        scope: &rayon::Scope<'scope>,
        capacity: usize,
    ) -> Self {
        let (sender1, receiver1) = bounded(3);
        let (sender2, receiver2) = bounded(3);
        let (flush_sender, flush_receiver) = bounded(0);

        for _ in 0..2 {
            sender1
                .send(Ok(Buf::new(capacity)))
                .expect("Failed to send buffer 1");
        }

        scope.spawn(move |_| write_thread(writer, sender1, receiver2, flush_sender));

        Self {
            _phantom: PhantomData,
            current_buf: Some(Buf::new(capacity)),
            receiver: receiver1,
            sender: sender2,
            flush_result_receiver: flush_receiver,
            eof: false,
        }
    }
}

impl<W: Write> Drop for RayonWriter<W> {
    fn drop(&mut self) {
        if let Some(buf) = self.current_buf.take() {
            self.sender.send(buf).expect("Failed to send buffer");
        }

        let mut buf = Buf::new(0);
        buf.flush = true;
        buf.eof = true;
        self.sender.send(buf).expect("Failed to send buffer");
        dbg!("Send eof");

        self.flush_result_receiver
            .recv()
            .expect("Failed to receive flush result")
            .expect("Failed to flush");
    }
}

impl<W: Write> Write for RayonWriter<W> {
    fn flush(&mut self) -> Result<()> {
        if self.eof {
            return Ok(());
        }

        if let Some(mut buf) = self.current_buf.take() {
            buf.flush = true;
            self.sender.send(buf).expect("Failed to send buffer");
        }

        self.flush_result_receiver
            .recv()
            .expect("Failed to receive flush result")?;

        Ok(())
    }
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.eof {
            return Ok(0);
        }
        if self.current_buf.is_none() {
            match self.receiver.recv().expect("Failed to receive buffer") {
                Ok(mut buffer) => {
                    buffer.clear();
                    self.current_buf = Some(buffer)
                }
                Err(e) => {
                    self.eof = true;
                    return Err(e);
                }
            }
        }

        let current_buf = self.current_buf.as_mut().unwrap();
        let copy_len = buf.len().min(current_buf.unfilled_buf().len());
        current_buf.unfilled_buf()[..copy_len].copy_from_slice(&buf[..copy_len]);
        current_buf.advance(copy_len);
        if current_buf.unfilled_buf().is_empty() {
            self.sender
                .send(self.current_buf.take().unwrap())
                .expect("Failed to send buffer");
        }

        Ok(copy_len)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct AlwaysErrorReader;
    impl Read for AlwaysErrorReader {
        fn read(&mut self, _buf: &mut [u8]) -> Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "AlwaysErrorReader",
            ))
        }
    }

    #[test]
    fn test_rayon_reader1() -> anyhow::Result<()> {
        // Read all
        let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
        let mut reader =
            super::RayonReader::with_capacity(std::io::Cursor::new(expected_data), 101);
        let mut loaded_data = Vec::new();
        reader.read_to_end(&mut loaded_data)?;
        assert_eq!(expected_data.len(), loaded_data.len());
        assert_eq!(&expected_data[..], &loaded_data[..]);

        // Read error
        let mut reader = super::RayonReader::new(AlwaysErrorReader);
        let mut loaded_data = [0u8; 1000];
        let s = reader.read_exact(&mut loaded_data);
        assert!(s.is_err());
        Ok(())
    }

    #[test]
    fn test_rayon_reader2() -> anyhow::Result<()> {
        // Read first 1000 bytes
        let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
        let mut reader =
            super::RayonReader::with_capacity(std::io::Cursor::new(expected_data), 101);
        let mut loaded_data = [0u8; 1000];
        reader.read_exact(&mut loaded_data)?;
        std::mem::drop(reader);
        assert_eq!(&expected_data[..1000], &loaded_data[..]);
        Ok(())
    }

    #[test]
    fn test_rayon_writer1() -> anyhow::Result<()> {
        // write test
        let expected_data = include_bytes!("../../../testfiles/sqlite3.c");
        let mut write_buffer = vec![];
        rayon::scope(|s| {
            let mut writer = RayonWriter::with_scope(&mut write_buffer, s);
            writer.write_all(expected_data).expect("write all");
            writer.flush().expect("flush");
        });
        assert_eq!(expected_data.len(), write_buffer.len());
        assert_eq!(&expected_data[..], &write_buffer[..]);

        Ok(())
    }

    #[test]
    fn test_rayon_writer2() -> anyhow::Result<()> {
        // write test
        let expected_data = include_bytes!("../../../testfiles/sqlite3.c");

        let mut write_buffer = vec![];
        rayon::scope(|s| {
            let mut writer = RayonWriter::with_scope(&mut write_buffer, s);
            writer.write_all(expected_data).expect("write all");
        });
        assert_eq!(expected_data.len(), write_buffer.len());
        assert_eq!(&expected_data[..], &write_buffer[..]);
        Ok(())
    }

    #[test]
    fn test_rayon_writer3() -> anyhow::Result<()> {
        // write test
        let expected_data = include_bytes!("../../../testfiles/sqlite3.c");

        let mut write_buffer = vec![];
        rayon::scope(|s| {
            let mut writer = RayonWriter::with_scope_and_capacity(&mut write_buffer, s, 101);
            writer.write_all(expected_data).expect("write all");
        });
        assert_eq!(expected_data.len(), write_buffer.len());
        assert_eq!(&expected_data[..], &write_buffer[..]);
        Ok(())
    }
}
