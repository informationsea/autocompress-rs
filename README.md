autocompress-rs
===============
[![Build](https://github.com/informationsea/autocompress-rs/actions/workflows/build.yml/badge.svg)](https://github.com/informationsea/autocompress-rs/actions/workflows/build.yml)
![GitHub](https://img.shields.io/github/license/informationsea/autocompress-rs)
![GitHub top language](https://img.shields.io/github/languages/top/informationsea/autocompress-rs)
[![Crates.io](https://img.shields.io/crates/v/autocompress)](https://crates.io/crates/autocompress)
[![Docs.rs](https://docs.rs/autocompress/badge.svg)](https://docs.rs/autocompress)

Automatically select suitable decoder from magic bytes or encoder from file extension.
This library also provides I/O thread pool to perform decompression and compression in background threads.

Supported file formats
---------------------

* [Gzip](https://www.ietf.org/rfc/rfc1952.txt)
* [Zlib](https://www.ietf.org/rfc/rfc1950.txt) (Cannot suggest format from magic bytes and file extension)
* [BZip2](https://www.sourceware.org/bzip2/)
* [XZ](https://tukaani.org/xz/format.html)
* [Snappy](https://github.com/google/snappy) (Cannot suggest format from file extension)
* [Z-standard](https://facebook.github.io/zstd/)
* [LZ4](https://www.lz4.org/)
* [Brotli](https://github.com/google/brotli) (Cannot suggest format from magic bytes)

Feature flags
-------------

* `default`, `full`: Enable all features.
* `gzip`: Enable gzip
* `snap`: Enable snappy
* `zstd`: Enable Z-standard
* `bzip2`: Enable bzip2
* `lz4`: Enable LZ4
* `xz`: Enable XZ
* `brotli`: Enable Brotli
* `thread`: Enable threaded I/O

Example
-------
```rust
use autocompress::open;
use std::io::{self, Read};

fn main() -> io::Result<()> {
  let mut buffer = Vec::new();
  open("testfiles/plain.txt.xz")?.read_to_end(&mut buffer)?;
  assert_eq!(buffer, b"ABCDEFG\r\n1234567");
  Ok(())
}
```
I/O thread example
------------------
 ```rust
use autocompress::{iothread::IoThread, open, create, CompressionLevel};
use std::io::{prelude::*, self};
fn main() -> io::Result<()> {
  let thread_pool = IoThread::new(2);
  let mut threaded_reader = thread_pool.open("testfiles/plain.txt.xz")?;
  let mut threaded_writer = thread_pool.create("target/plain.txt.xz", CompressionLevel::Default)?;
  let mut buffer = Vec::new();
  threaded_reader.read_to_end(&mut buffer)?;
  assert_eq!(buffer, b"ABCDEFG\r\n1234567");
  threaded_writer.write_all(b"ABCDEFG\r\n1234567")?;
  Ok(())
}
 ```