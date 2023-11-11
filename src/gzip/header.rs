use crate::{ReadExt, WriteExt};
use std::io::{BufRead, Write};

/// This type represents the header of a gzip file.
///
/// Please read gzip specification for more information.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GzipHeader {
    pub compression_method: u8,
    pub flag: u8,
    pub mtime: u32,
    pub extra_flag: u8,
    pub os: GzipOs,
    pub xlen: u16,
    pub extra: Option<Vec<u8>>,
    pub fname: Option<Vec<u8>>,
    pub fcomment: Option<Vec<u8>>,
    pub crc16: Option<u16>,
    pub header_size: usize,
}

pub const FTEXT: u8 = 0b0000_0001;
pub const FHCRC: u8 = 0b0000_0010;
pub const FEXTRA: u8 = 0b0000_0100;
pub const FNAME: u8 = 0b0000_1000;
pub const FCOMMENT: u8 = 0b0001_0000;

/// Operation System of gzip file created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GzipOs {
    FatFs = 0,
    Amiga = 1,
    Vms = 2,
    Unix = 3,
    VmCms = 4,
    AtariTos = 5,
    Hpfs = 6,
    Macintosh = 7,
    ZSystem = 8,
    Cpm = 9,
    Tops20 = 10,
    Ntfs = 11,
    Qdos = 12,
    AcornRiscos = 13,
    Unknown = 255,
}

impl From<u8> for GzipOs {
    fn from(value: u8) -> Self {
        match value {
            0 => GzipOs::FatFs,
            1 => GzipOs::Amiga,
            2 => GzipOs::Vms,
            3 => GzipOs::Unix,
            4 => GzipOs::VmCms,
            5 => GzipOs::AtariTos,
            6 => GzipOs::Hpfs,
            7 => GzipOs::Macintosh,
            8 => GzipOs::ZSystem,
            9 => GzipOs::Cpm,
            10 => GzipOs::Tops20,
            11 => GzipOs::Ntfs,
            12 => GzipOs::Qdos,
            13 => GzipOs::AcornRiscos,
            _ => GzipOs::Unknown,
        }
    }
}

impl Default for GzipHeader {
    fn default() -> Self {
        Self {
            compression_method: 8,
            flag: 0,
            mtime: 0,
            extra_flag: 0,
            os: GzipOs::Unknown,
            xlen: 0,
            extra: None,
            fname: None,
            fcomment: None,
            crc16: None,
            header_size: 10,
        }
    }
}

impl GzipHeader {
    /// Write a gzip header to a writer.
    pub fn write<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
        // gzip signature
        writer.write_all(&[0x1f, 0x8b])?;
        writer.write_u8(self.compression_method)?;
        writer.write_u8(self.flag)?;
        writer.write_u32_le(self.mtime)?;
        writer.write_u8(self.extra_flag)?;
        writer.write_u8(self.os as u8)?;

        if self.flag & FEXTRA != 0 {
            if let Some(extra_data) = self.extra.as_ref() {
                writer.write_u16_le(extra_data.len().try_into().map_err(|_e| {
                    std::io::Error::new(std::io::ErrorKind::Other, "Too large extra data")
                })?)?;
                writer.write_all(extra_data)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No extra data in header with FEXTRA flag",
                ));
            }
        }

        if self.flag & FNAME != 0 {
            if let Some(fname) = self.fname.as_ref() {
                writer.write_all(fname)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No fname with FNAME flag",
                ));
            }
        }

        if self.flag & FCOMMENT != 0 {
            if let Some(fcomment) = self.fcomment.as_ref() {
                writer.write_all(fcomment)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No comment with FCOMMENT flag",
                ));
            }
        }

        if self.flag & FHCRC != 0 {
            if let Some(crc16) = self.crc16 {
                writer.write_u16_le(crc16)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No crc16 with FHCRC flag",
                ));
            }
        }

        Ok(())
    }

    /// Parse a gzip header from a reader.
    pub fn parse<R: BufRead>(mut reader: R) -> std::io::Result<Self> {
        let mut ids = [0u8; 2];
        reader.read_exact(&mut ids)?;
        if ids != [0x1f, 0x8b] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid gzip header",
            ));
        }

        let compression_method = reader.read_u8()?;
        let flag = reader.read_u8()?;
        let mtime = reader.read_u32_le()?;
        let extra_flag = reader.read_u8()?;
        let os: GzipOs = reader.read_u8()?.into();

        let mut header_size = 2 + 1 + 1 + 4 + 1 + 1;

        let (xlen, extra) = if flag & FEXTRA != 0 {
            let xlen = reader.read_u16_le()?;
            let mut extra = vec![0u8; xlen as usize];
            reader.read_exact(&mut extra)?;
            header_size += 2 + xlen as usize;
            (xlen, Some(extra))
        } else {
            (0, None)
        };

        let fname = if flag & FNAME != 0 {
            let mut fname = Vec::new();
            reader.read_until(0, &mut fname)?;
            if !fname.ends_with(b"\0") {
                //eprintln!("incomplete fname: {:?}", fname);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "incomplete gzip header",
                ));
            }
            header_size += fname.len();
            Some(fname)
        } else {
            None
        };

        let fcomment = if flag & FCOMMENT != 0 {
            let mut fcomment = Vec::new();
            reader.read_until(0, &mut fcomment)?;
            if !fcomment.ends_with(b"\0") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "incomplete gzip header",
                ));
            }
            header_size += fcomment.len();
            Some(fcomment)
        } else {
            None
        };

        let crc16 = if flag & FHCRC != 0 {
            header_size += 2;
            Some(reader.read_u16_le()?)
        } else {
            None
        };

        Ok(GzipHeader {
            compression_method,
            flag,
            mtime,
            extra_flag,
            os,
            xlen,
            extra,
            fname,
            fcomment,
            crc16,
            header_size,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;
    use std::io::{BufReader, Read, Seek};
    use std::path::Path;

    #[tokio::test]
    async fn test_gzip_header() -> anyhow::Result<()> {
        let expected_header = GzipHeader {
            compression_method: 8,
            flag: FNAME,
            mtime: 1684244723,
            extra_flag: 0,
            os: GzipOs::Unix,
            xlen: 0,
            extra: None,
            fname: Some(b"sqlite3.c\0".to_vec()),
            fcomment: None,
            crc16: None,
            header_size: 10 + 10,
        };

        let mut reader = BufReader::new(File::open("testfiles/sqlite3.c.gz")?);
        let header = GzipHeader::parse(&mut reader)?;
        assert_eq!(header, expected_header);

        let mut reader = BufReader::new(File::open("testfiles/sqlite3.c.pigz.gz")?);
        let header = GzipHeader::parse(&mut reader)?;
        assert_eq!(header, expected_header);

        let expected_header = GzipHeader {
            compression_method: 8,
            flag: 0,
            mtime: 1684244723,
            extra_flag: 0,
            os: GzipOs::Unix,
            xlen: 0,
            extra: None,
            fname: None,
            fcomment: None,
            crc16: None,
            header_size: 10,
        };

        let mut reader = BufReader::new(File::open("testfiles/sqlite3.c.pipe.gz")?);
        let header = GzipHeader::parse(&mut reader)?;
        assert_eq!(header, expected_header);

        let expected_header = GzipHeader {
            compression_method: 8,
            flag: FEXTRA,
            mtime: 0,
            extra_flag: 0,
            os: GzipOs::Unknown,
            xlen: 6,
            extra: Some(vec![66, 67, 2, 0, 0x13, 0x4a]),
            fname: None,
            fcomment: None,
            crc16: None,
            header_size: 10 + 8,
        };

        let mut reader = BufReader::new(File::open("testfiles/sqlite3.c.bgzip.gz")?);
        let header = GzipHeader::parse(&mut reader)?;
        assert_eq!(header, expected_header);

        Ok(())
    }

    fn test_header_write_one_file<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
        let mut reader = BufReader::new(File::open(path)?);
        let header = GzipHeader::parse(&mut reader)?;
        reader.seek(std::io::SeekFrom::Start(0))?;
        let mut expected_data = vec![0u8; header.header_size];
        reader.read_exact(&mut expected_data)?;
        let mut generated_data = Vec::new();
        header.write(&mut generated_data)?;
        assert_eq!(expected_data, generated_data);
        Ok(())
    }

    #[test]
    fn test_gzip_header_write() -> anyhow::Result<()> {
        test_header_write_one_file("testfiles/sqlite3.c.gz")?;
        test_header_write_one_file("testfiles/sqlite3.c.pigz.gz")?;
        test_header_write_one_file("testfiles/sqlite3.c.bgzip.gz")?;
        test_header_write_one_file("testfiles/sqlite3.c.pipe.gz")?;
        Ok(())
    }
}
