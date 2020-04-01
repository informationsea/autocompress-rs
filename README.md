autocompress-rs
===============

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