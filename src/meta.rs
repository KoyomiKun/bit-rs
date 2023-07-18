use std::mem;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

pub(crate) type Fid = u32;

pub(crate) const LOG_HEADER_MAX_SIZE: usize =
    LOG_FIXED_HEADER_SIZE + length_delimiter_len(std::u32::MAX as usize) * 2;
pub(crate) const LOG_FIXED_HEADER_SIZE: usize = mem::size_of::<u8>();
pub(crate) const LOG_FIXED_CRC_SIZE: usize = mem::size_of::<u32>();

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LogPos {
    pub(crate) fid: u32,
    pub(crate) offset: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordType {
    Delete,
    Normal,
}

impl RecordType {
    pub(crate) fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x00 => Ok(Self::Delete),
            0x01 => Ok(Self::Normal),
            _ => Err(anyhow!("invalid record type")),
        }
    }

    pub(crate) fn to_u8(self) -> u8 {
        match self {
            RecordType::Delete => 0x00,
            RecordType::Normal => 0x01,
        }
    }
}

// log entry
pub(crate) struct LogRecord {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) typ: RecordType,
}

impl LogRecord {
    pub(crate) fn serde(&self) -> Result<(Bytes, u32)> {
        let l = self.serde_len();
        let mut buf = BytesMut::with_capacity(l);

        buf.put_u8(self.typ.to_u8());
        encode_length_delimiter(self.key.len(), &mut buf)?;
        encode_length_delimiter(self.value.len(), &mut buf)?;

        buf.extend(self.key);
        buf.extend(self.value);

        let crc = crc32fast::hash(buf.get(..l - LOG_FIXED_CRC_SIZE).unwrap());
        buf.put_u32(crc);
        Ok((buf.freeze(), crc))
    }

    pub(crate) fn serde_len(&self) -> usize {
        LOG_FIXED_HEADER_SIZE
            + LOG_FIXED_CRC_SIZE
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
    }
}
