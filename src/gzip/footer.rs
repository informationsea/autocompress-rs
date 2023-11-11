use crate::{ReadExt, WriteExt};
use std::io;

/// This type represents the footer of a gzip file.
///
/// Please read gzip specification for more information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GzipFooter {
    pub crc32: u32,
    pub isize: u32,
}

impl GzipFooter {
    /// Parse a gzip footer from a reader.
    pub fn parse(mut reader: impl io::BufRead) -> io::Result<Self> {
        let crc32 = reader.read_u32_le()?;
        let isize = reader.read_u32_le()?;
        Ok(Self { crc32, isize })
    }

    pub fn footer_size(&self) -> usize {
        8
    }

    pub fn write(&self, mut writer: impl io::Write) -> io::Result<()> {
        writer.write_u32_le(self.crc32)?;
        writer.write_u32_le(self.isize)?;
        Ok(())
    }
}
