use super::super::CompressionLevel;
use super::{IoRequest, IoResult};
use std::collections::HashMap;
use std::io::{self, prelude::*};

/// Threaded writer. Creates with [`super::IoThread::add_block_writer`].
pub struct BlockWriter<'a, W: io::Write + Send> {
    pub(crate) block_compress: BlockCompress,
    pub(crate) sender: &'a crossbeam_channel::Sender<IoRequest>,
    pub(crate) result_receiver: crossbeam_channel::Receiver<IoResult>,
    pub(crate) result_sender: crossbeam_channel::Sender<IoResult>,
    pub(crate) current_buffer: Option<Vec<u8>>,
    pub(crate) empty_buffer: Vec<Vec<u8>>,
    pub(crate) compressed_result_buffer: HashMap<u64, Vec<u8>>,
    pub(crate) next_write_index: u64,
    pub(crate) next_compress_index: u64,
    pub(crate) writer: Option<Box<dyn Write + Send>>,
    pub(crate) closed: bool,
    pub(crate) _phantom: std::marker::PhantomData<W>,
}

impl<'a, W: io::Write + Send> BlockWriter<'a, W> {
    fn submit_write_if_required(&mut self) -> io::Result<()> {
        // for k in self.compressed_result_buffer.iter() {
        //     log::trace!(
        //         "compressed result buffer: {} {}",
        //         k.0,
        //         self.next_write_index
        //     );
        // }
        if self
            .compressed_result_buffer
            .contains_key(&self.next_write_index)
        {
            //log::debug!("found: {}", self.next_write_index);
            if let Some(writer) = self.writer.take() {
                //log::debug!("submit write request: {}", self.next_write_index);
                self.sender
                    .send(IoRequest::Write(
                        writer,
                        self.compressed_result_buffer
                            .remove(&self.next_write_index)
                            .unwrap(),
                        self.result_sender.clone(),
                    ))
                    .map_err(|_e| {
                        io::Error::new(io::ErrorKind::Other, "failed to send write request")
                    })?;
                self.next_write_index += 1;
            }
        }
        Ok(())
    }

    #[inline]
    fn push_empty_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.current_buffer.is_none() {
            self.current_buffer = Some(buffer);
        } else {
            self.empty_buffer.push(buffer);
        }
    }

    fn process_recv(&mut self, result: IoResult) -> io::Result<()> {
        match result {
            IoResult::Flush(writer, result) => {
                self.writer = Some(writer);
                if let Err(e) = result {
                    return Err(e);
                }
            }
            IoResult::Write(writer, buf, result) => {
                log::trace!("block write result");
                self.writer = Some(writer);
                self.push_empty_buffer(buf);
                if let Err(e) = result {
                    return Err(e);
                }
            }
            IoResult::Compress(index, raw_buf, compressed_buf) => {
                log::trace!("block compress result: {}", index);
                self.compressed_result_buffer.insert(index, compressed_buf);
                self.push_empty_buffer(raw_buf);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn recv_result(
        &mut self,
        buffer_required: bool,
        empty_buffer_required: bool,
        writer_required: bool,
        force_receive: bool,
    ) -> io::Result<()> {
        if self.current_buffer.is_none() && !self.empty_buffer.is_empty() {
            self.current_buffer = Some(self.empty_buffer.remove(0));
        }

        if force_receive {
            match self.result_receiver.recv() {
                Ok(result) => {
                    self.process_recv(result)?;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
            self.submit_write_if_required()?;
        }

        loop {
            match self.result_receiver.try_recv() {
                Ok(result) => self.process_recv(result)?,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        }

        self.submit_write_if_required()?;

        while (buffer_required && self.current_buffer.is_none())
            || (empty_buffer_required && self.empty_buffer.is_empty())
            || (writer_required && self.writer.is_none())
        {
            match self.result_receiver.recv() {
                Ok(result) => {
                    self.process_recv(result)?;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
            self.submit_write_if_required()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        log::trace!("block close: {:?}", self.closed);
        if self.closed {
            return Ok(());
        }

        if let Some(send_buffer) = self.current_buffer.take() {
            //log::trace!("close send buffer: {:?}", send_buffer);
            if !send_buffer.is_empty() {
                self.sender
                    .send(IoRequest::Compress(
                        self.block_compress,
                        self.next_compress_index,
                        send_buffer,
                        self.empty_buffer.pop().unwrap(),
                        self.result_sender.clone(),
                    ))
                    .map_err(|_e| {
                        io::Error::new(io::ErrorKind::Other, "failed to send compress request")
                    })?;
                self.next_compress_index += 1;
            }
        }

        // log::debug!(
        //     "write all: {} {}",
        //     self.next_compress_index,
        //     self.next_write_index
        // );
        while self.next_compress_index != self.next_write_index {
            // log::debug!(
            //     "write all: {} {}",
            //     self.next_compress_index,
            //     self.next_write_index
            // );
            self.recv_result(true, true, true, true)?;
        }

        self.closed = true;

        // drop writer and buffer
        self.writer.take();
        self.empty_buffer.clear();
        Ok(())
    }
}

impl<'a, W: io::Write + Send> Drop for BlockWriter<'a, W> {
    fn drop(&mut self) {
        log::trace!("block drop");
        self.close().expect("Failed to close writer");
    }
}

impl<'a, W: io::Write + Send> Write for BlockWriter<'a, W> {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        let original_len = buf.len();
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Already closed"));
        }

        //log::debug!("write call {} {}", buf.len(), str::from_utf8(buf).unwrap());
        self.recv_result(true, true, false, false)?;

        while self.block_compress.block_size - self.current_buffer.as_ref().unwrap().len()
            < buf.len()
        {
            self.recv_result(true, true, true, false)?;
            // log::debug!(
            //     "write send: {} {}",
            //     self.block_compress.block_size - self.current_buffer.as_ref().unwrap().len(),
            //     buf.len()
            // );
            let mut send_buffer = self.current_buffer.take().unwrap();

            let remain_bytes = self.block_compress.block_size - send_buffer.len();
            send_buffer.extend_from_slice(&buf[..remain_bytes]);
            //log::debug!("send buffer: {:?}", str::from_utf8(&send_buffer).unwrap());
            self.sender
                .send(IoRequest::Compress(
                    self.block_compress,
                    self.next_compress_index,
                    send_buffer,
                    self.empty_buffer.pop().unwrap(),
                    self.result_sender.clone(),
                ))
                .map_err(|_e| {
                    io::Error::new(io::ErrorKind::Other, "failed to send compress request")
                })?;
            self.next_compress_index += 1;
            buf = &buf[remain_bytes..];
            // log::debug!("slim write buf: {:?}", str::from_utf8(buf).unwrap());
            self.recv_result(true, true, true, false)?;
        }

        // log::debug!("write 1: {}", str::from_utf8(&buf).unwrap());
        self.current_buffer.as_mut().unwrap().extend_from_slice(buf);
        Ok(original_len)
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Already closed"));
        }
        if let Some(send_buffer) = self.current_buffer.take() {
            self.sender
                .send(IoRequest::Compress(
                    self.block_compress,
                    self.next_compress_index,
                    send_buffer,
                    self.empty_buffer.pop().unwrap(),
                    self.result_sender.clone(),
                ))
                .map_err(|_e| {
                    io::Error::new(io::ErrorKind::Other, "failed to send compress request")
                })?;
            self.next_write_index += 1;
        }
        if self.next_compress_index != self.next_write_index {
            self.recv_result(false, true, true, false)?;
        }
        let writer = self.writer.take().unwrap();
        self.sender
            .send(IoRequest::Flush(writer, self.result_sender.clone()))
            .map_err(|_e| io::Error::new(io::ErrorKind::Other, "failed to send flush request"))?;
        Ok(())
    }
}

type CompressBlock = fn(block: &[u8], result: &mut Vec<u8>, compression_level: CompressionLevel);

/// Methods for threaded compression
#[derive(Clone, Copy)]
pub struct BlockCompress {
    pub(crate) compress_block: CompressBlock,
    pub(crate) block_size: usize,
    pub(crate) compression_level: CompressionLevel,
}

impl BlockCompress {
    pub fn plain() -> Self {
        BlockCompress {
            compress_block: plain_compress_block,
            block_size: 1024 * 128,
            compression_level: CompressionLevel::Default,
        }
    }

    #[cfg(feature = "flate2")]
    pub fn gzip(compression_level: CompressionLevel) -> Self {
        BlockCompress {
            compress_block: gzip_compress_block,
            block_size: 1024 * 128,
            compression_level,
        }
    }

    #[cfg(feature = "bzip2")]
    pub fn bzip2(compression_level: CompressionLevel) -> Self {
        BlockCompress {
            compress_block: bzip2_compress_block,
            block_size: 1024 * 128,
            compression_level,
        }
    }

    #[cfg(feature = "xz2")]
    pub fn xz(compression_level: CompressionLevel) -> Self {
        BlockCompress {
            compress_block: xz_compress_block,
            block_size: 1024 * 128,
            compression_level,
        }
    }

    #[cfg(feature = "zstd")]
    pub fn zstd(compression_level: CompressionLevel) -> Self {
        BlockCompress {
            compress_block: zstd_compress_block,
            block_size: 1024 * 128,
            compression_level,
        }
    }
}

fn plain_compress_block(block: &[u8], result: &mut Vec<u8>, _compression_level: CompressionLevel) {
    result.extend_from_slice(block)
}

#[cfg(feature = "flate2")]
fn gzip_compress_block(block: &[u8], result: &mut Vec<u8>, compression_level: CompressionLevel) {
    let mut writer = flate2::write::GzEncoder::new(result, compression_level.into());
    writer.write_all(block).unwrap();
}

#[cfg(feature = "bzip2")]
fn bzip2_compress_block(block: &[u8], result: &mut Vec<u8>, compression_level: CompressionLevel) {
    let mut writer = bzip2::write::BzEncoder::new(result, compression_level.into());
    writer.write_all(block).unwrap();
}

#[cfg(feature = "xz2")]
fn xz_compress_block(block: &[u8], result: &mut Vec<u8>, compression_level: CompressionLevel) {
    let mut writer = xz2::write::XzEncoder::new(result, compression_level.xz_level());
    writer.write_all(block).unwrap();
    writer.finish().unwrap();
}

#[cfg(feature = "zstd")]
fn zstd_compress_block(block: &[u8], result: &mut Vec<u8>, compression_level: CompressionLevel) {
    let mut compressed = zstd::bulk::compress(block, compression_level.zstd_level()).unwrap();
    result.append(&mut compressed);
}

#[cfg(test)]
mod test {
    use super::super::*;
    use super::*;
    use std::str;

    #[test]
    fn test_gzip_block_writer() -> io::Result<()> {
        // std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut gzip_block_compress = BlockCompress::gzip(CompressionLevel::Default);
        gzip_block_compress.block_size = 20;
        let file_writer = std::fs::File::create("target/block1.txt.gz")?;
        let iothread = IoThread::new(1);
        let mut block_writer = iothread.add_block_writer(file_writer, gzip_block_compress);
        block_writer.write_all(b"a123456789")?;
        block_writer.write_all(b"b123456789")?;
        block_writer.write_all(b"c123456789")?;
        block_writer.write_all(b"d123456789")?;
        block_writer.write_all(b"e123456789ABC")?;
        block_writer.write_all(b"f123456789ABC")?;
        block_writer.write_all(b"g123456789ABC")?;
        block_writer.write_all(b"h123456789ABC")?;
        block_writer.write_all(b"i123456789j123456789k123456789l123456789")?;
        std::mem::drop(block_writer);

        let mut file_reader = crate::open("target/block1.txt.gz")?;
        let mut wrote_bytes = Vec::new();
        file_reader.read_to_end(&mut wrote_bytes)?;
        assert_eq!(
            str::from_utf8(&wrote_bytes).unwrap(),
            "a123456789b123456789c123456789d123456789e123456789ABCf123456789ABCg123456789ABCh123456789ABCi123456789j123456789k123456789l123456789"
        );

        Ok(())
    }

    #[test]
    fn test_bzip2_block_writer() -> io::Result<()> {
        // std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut block_compress = BlockCompress::bzip2(CompressionLevel::Default);
        block_compress.block_size = 20;
        let file_writer = std::fs::File::create("target/block1.txt.bz2")?;
        let iothread = IoThread::new(1);
        let mut block_writer = iothread.add_block_writer(file_writer, block_compress);
        block_writer.write_all(b"a123456789")?;
        block_writer.write_all(b"b123456789")?;
        block_writer.write_all(b"c123456789")?;
        block_writer.write_all(b"d123456789")?;
        block_writer.write_all(b"e123456789ABC")?;
        block_writer.write_all(b"f123456789ABC")?;
        block_writer.write_all(b"g123456789ABC")?;
        block_writer.write_all(b"h123456789ABC")?;
        block_writer.write_all(b"i123456789j123456789k123456789l123456789")?;
        std::mem::drop(block_writer);

        let mut file_reader = crate::open("target/block1.txt.bz2")?;
        let mut wrote_bytes = Vec::new();
        file_reader.read_to_end(&mut wrote_bytes)?;
        assert_eq!(
            str::from_utf8(&wrote_bytes).unwrap(),
            "a123456789b123456789c123456789d123456789e123456789ABCf123456789ABCg123456789ABCh123456789ABCi123456789j123456789k123456789l123456789"
        );

        Ok(())
    }

    #[test]
    fn test_xz_block_writer() -> io::Result<()> {
        // std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut block_compress = BlockCompress::xz(CompressionLevel::Default);
        block_compress.block_size = 20;
        let file_writer = std::fs::File::create("target/block1.txt.xz")?;
        let iothread = IoThread::new(1);
        let mut block_writer = iothread.add_block_writer(file_writer, block_compress);

        block_writer.write_all(b"a123456789")?;
        block_writer.write_all(b"b123456789")?;
        block_writer.write_all(b"c123456789")?;
        block_writer.write_all(b"d123456789")?;
        block_writer.write_all(b"e123456789ABC")?;
        block_writer.write_all(b"f123456789ABC")?;
        block_writer.write_all(b"g123456789ABC")?;
        block_writer.write_all(b"h123456789ABC")?;
        block_writer.write_all(b"i123456789j123456789k123456789l123456789")?;
        std::mem::drop(block_writer);

        let mut file_reader = crate::open("target/block1.txt.xz")?;
        let mut wrote_bytes = Vec::new();
        file_reader.read_to_end(&mut wrote_bytes)?;
        assert_eq!(
            str::from_utf8(&wrote_bytes).unwrap(),
            "a123456789b123456789c123456789d123456789e123456789ABCf123456789ABCg123456789ABCh123456789ABCi123456789j123456789k123456789l123456789"
        );

        Ok(())
    }

    #[test]
    fn test_zstd_block_writer() -> io::Result<()> {
        // std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut block_compress = BlockCompress::zstd(CompressionLevel::Default);
        block_compress.block_size = 20;
        let file_writer = std::fs::File::create("target/block1.txt.zst")?;
        let iothread = IoThread::new(1);
        let mut block_writer = iothread.add_block_writer(file_writer, block_compress);

        block_writer.write_all(b"a123456789")?;
        block_writer.write_all(b"b123456789")?;
        block_writer.write_all(b"c123456789")?;
        block_writer.write_all(b"d123456789")?;
        block_writer.write_all(b"e123456789ABC")?;
        block_writer.write_all(b"f123456789ABC")?;
        block_writer.write_all(b"g123456789ABC")?;
        block_writer.write_all(b"h123456789ABC")?;
        block_writer.write_all(b"i123456789j123456789k123456789l123456789")?;
        std::mem::drop(block_writer);
        eprintln!("drop writer");

        let mut file_reader = crate::open("target/block1.txt.zst")?;
        let mut wrote_bytes = Vec::new();
        file_reader.read_to_end(&mut wrote_bytes)?;
        assert_eq!(
            str::from_utf8(&wrote_bytes).unwrap(),
            "a123456789b123456789c123456789d123456789e123456789ABCf123456789ABCg123456789ABCh123456789ABCi123456789j123456789k123456789l123456789"
        );

        Ok(())
    }
}
