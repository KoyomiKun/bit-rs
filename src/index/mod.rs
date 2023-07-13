use anyhow::Result;

use crate::meta::LogPos;

pub(crate) mod btree;

pub(crate) trait Index: Sync + Send {
    fn put(&mut self, key: Vec<u8>, pos: LogPos) -> Result<()>;
    fn get(&self, key: Vec<u8>) -> Result<LogPos>;
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;
}
