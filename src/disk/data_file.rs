use std::path::Path;

use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::meta::{
    Fid, LogRecord, RecordType, LOG_FIXED_CRC_SIZE, LOG_FIXED_HEADER_SIZE, LOG_HEADER_MAX_SIZE,
};

use super::io::{MMap, IO};

// DataFile is an
pub(crate) struct DataFile {
    fid: Fid,
    io: Box<dyn IO>,
    size: usize,
}

impl DataFile {
    pub(crate) fn new(dir_path: &Path, fid: Fid) -> Result<Self> {
        let path = dir_path.join(format!("{:10}.data", fid));
        let io = Box::new(MMap::new(path.as_path()).map_err(|e| anyhow!("create io failed: {e}"))?);
        let size = io.size();

        Ok(Self { fid, io, size })
    }

    pub(crate) async fn append_record(&mut self, record: &LogRecord) -> Result<usize> {
        let (serded_lr, _) = record.serde()?;
        let size = self.io.append(&serded_lr)?;
        self.size += size;
        Ok(size)
    }

    /// read_bytes read record bytes including header and data
    /// HINT: read delete record may encounter EOF error, whiling this record is
    /// at the last position shorter than MAX_HEADER_SIZE
    pub(crate) async fn read_record(&self, offset: usize) -> Result<(LogRecord, usize)> {
        // read header from bytes
        let mut header_buf = BytesMut::zeroed(LOG_HEADER_MAX_SIZE);
        self.io.read(&mut header_buf, offset)?;

        let typ = RecordType::from_u8(header_buf.get_u8())?;

        let k_size = decode_length_delimiter(&mut header_buf)?;
        let v_size = decode_length_delimiter(&mut header_buf)?;

        if k_size == 0 && v_size == 0 {
            return Err(anyhow!("read EOF"));
        }

        let actual_header_len =
            LOG_FIXED_HEADER_SIZE + length_delimiter_len(k_size) + length_delimiter_len(v_size);

        let mut kv_buf = BytesMut::zeroed(k_size + v_size + LOG_FIXED_CRC_SIZE);
        self.io.read(&mut kv_buf, offset + actual_header_len)?;

        let key = Bytes::copy_from_slice(kv_buf.get(..k_size).unwrap());
        let value = Bytes::copy_from_slice(kv_buf.get(k_size..k_size + v_size).unwrap());

        kv_buf.advance(k_size + v_size);

        let crc = kv_buf.get_u32();
        let rc = LogRecord { key, value, typ };
        let (_, expected_crc) = rc
            .serde()
            .map_err(|e| anyhow!("serde record failed: {e}"))?;

        if expected_crc != crc {
            return Err(anyhow!("crc check failed"));
        }
        Ok((rc, actual_header_len + k_size + v_size))
    }

    pub(crate) async fn current_size(&self) -> usize {
        self.size
    }

    pub(crate) async fn fsync(&self) -> Result<()> {
        self.io.sync()
    }

    pub(crate) fn fid(&self) -> Fid {
        self.fid
    }
}
