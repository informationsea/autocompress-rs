autocompress-rs
===============
[![Build status](https://ci.appveyor.com/api/projects/status/x62tbcohmc7or8f1?svg=true)](https://ci.appveyor.com/project/informationsea/autocompress-rs)
[![Build Status](https://travis-ci.org/informationsea/autocompress-rs.svg?branch=master)](https://travis-ci.org/informationsea/autocompress-rs)
![GitHub](https://img.shields.io/github/license/informationsea/autocompress-rs)
![GitHub top language](https://img.shields.io/github/languages/top/informationsea/autocompress-rs)
[![Crates.io](https://img.shields.io/crates/v/autocompress)](https://crates.io/crates/autocompress)
[![Docs.rs](https://docs.rs/autocompress/badge.svg)](https://docs.rs/autocompress)

Automatically select suitable decoder from magic bytes or encoder from file extension.

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