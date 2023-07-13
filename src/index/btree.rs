use anyhow::Result;

use crate::index::Index;
use crate::meta::LogPos;

pub(crate) struct BTreeIndex {}

impl Index for BTreeIndex {
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

impl BTreeIndex {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
