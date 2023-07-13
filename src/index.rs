use anyhow::Result;

use crate::meta::LogPos;

pub(crate) trait Indexer: Sync + Send {
    fn put(&mut self, key: Vec<u8>, pos: LogPos) -> Result<()>;
    fn get(&self, key: Vec<u8>) -> Result<LogPos>;
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;
}

pub(crate) struct BTreeIndexer {}

impl Indexer for BTreeIndexer {
    fn put(&mut self, key: Vec<u8>, pos: LogPos) -> Result<()> {
        unimplemented!()
    }

    fn get(&self, key: Vec<u8>) -> Result<LogPos> {
        unimplemented!()
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        unimplemented!()
    }
}

impl BTreeIndexer {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
