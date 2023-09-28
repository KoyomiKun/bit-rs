use anyhow::Result;
use bytes::Bytes;

use crate::meta::LogPos;

pub(crate) mod btree;

pub(crate) trait Index: Sync + Send {
    fn put(&mut self, key: Bytes, pos: LogPos) -> Result<()>;
    fn get(&self, key: &Bytes) -> Option<LogPos>;
    fn delete(&mut self, key: &Bytes) -> Result<()>;
    fn iter(&self, opt: IndexOpt) -> Box<dyn IndexIter>;
}

pub(crate) trait IndexIter: Sync + Send {
    fn next(&mut self) -> Option<(Bytes, LogPos)>;
    fn seek(&mut self, key: Bytes);
    fn rewind(&mut self);
}

pub struct IndexOpt {
    reverse: bool,
    prefix: Bytes,
}
