use anyhow::Result;

pub(crate) trait IO {
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn sync(&self) -> Result<()>;
}

pub(crate) struct MMap {}

impl IO for MMap {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        unimplemented!()
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }
}
