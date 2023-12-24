# autocompress-rs

[![Build](https://github.com/informationsea/autocompress-rs/actions/workflows/build.yml/badge.svg)](https://github.com/informationsea/autocompress-rs/actions/workflows/build.yml)
![GitHub](https://img.shields.io/github/license/informationsea/autocompress-rs)
![GitHub top language](https://img.shields.io/github/languages/top/informationsea/autocompress-rs)
[![Crates.io](https://img.shields.io/crates/v/autocompress)](https://crates.io/crates/autocompress)
[![Docs.rs](https://docs.rs/autocompress/badge.svg)](https://docs.rs/autocompress)

Automatically select suitable decoder from magic bytes or encoder from file
extension. This library also provides I/O thread pool to perform decompression
and compression in background threads.

## Supported file formats

- [Gzip](https://www.ietf.org/rfc/rfc1952.txt)
- [Zlib](https://www.ietf.org/rfc/rfc1950.txt) (Cannot suggest format from magic
  bytes and file extension)
- [BZip2](https://www.sourceware.org/bzip2/)
- [XZ](https://tukaani.org/xz/format.html)
- [Z-standard](https://facebook.github.io/zstd/)

## Feature flags

- `gzip` : Gzip format support
- `bgzip` : [bgzip](https://github.com/informationsea/bgzip-rs) format support
- `bzip2` : Bzip2 format support
- `xz` : XZ format support
- `zstd` : Zstd format support
- `rayon` : Off-load compression and decompression process to another thread
  using [rayon](https://crates.io/crates/rayon)
- `tokio` : Async reader and writer support with
  [tokio](https://crates.io/crates/tokio)
- `tokio_fs`: Enable `autodetect_async_open` function

## Example

### Read from a file

```rust
use std::io::prelude::*;
use autocompress::autodetect_open;

fn main() -> anyhow::Result<()> {
    let mut reader = autodetect_open("testfiles/pg2701.txt.xz")?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    Ok(())
}
```

### Write to a file

```rust
use std::io::prelude::*;
use autocompress::{autodetect_create, CompressionLevel};

fn main() -> anyhow::Result<()> {
    let mut writer = autodetect_create("target/doc-index.xz", CompressionLevel::Default)?;
    writer.write_all(&b"Hello, world\n"[..])?;
    Ok(())
}
```

### Compress file in parallel

```rust
use std::io::prelude::*;
use autocompress::{autodetect_parallel_create, CompressionLevel};

fn main() -> anyhow::Result<()> {
  let mut writer = autodetect_parallel_create("target/doc-index2.xz", CompressionLevel::Default)?;
  writer.write_all(&b"Hello, world\n"[..])?;
  Ok(())
}
```