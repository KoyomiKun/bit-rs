use anyhow::Result;
use bytes::Bytes;

use crate::meta::LogPos;

pub(crate) mod btree;

pub(crate) trait Index: Sync + Send {
    fn put(&mut self, key: Bytes, pos: LogPos) -> Result<()>;
    fn get(&self, key: &Bytes) -> Option<LogPos>;
    fn delete(&mut self, key: &Bytes) -> Result<()>;
}

pub(crate) trait IndexIter: Sync + Send {
    fn next(&mut self) -> Option<LogPos>;
    fn seek(&mut self, pos: usize);
    fn rewind(&mut self);
}
