use std::path::Path;

use anyhow::Result;

pub(crate) trait IO {
    fn append(&self, buf: &[u8]) -> Result<usize>;
    fn read(&self, buf: &mut [u8], offset: usize) -> Result<usize>;
    fn sync(&self) -> Result<()>;
    fn size(&self) -> usize;
}

pub(crate) struct MMap {}

impl IO for MMap {
    fn read(&self, buf: &mut [u8], offset: usize) -> Result<usize> {
        unimplemented!()
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    fn append(&self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }
}

impl MMap {
    pub fn new(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}
