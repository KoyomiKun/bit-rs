use std::path::Path;

use anyhow::Result;

use crate::meta::{Fid, LogRecord};

pub(crate) struct DataFile {}

impl DataFile {
    pub(crate) fn new(dir_path: &str, fid: Fid) -> Self {
        let path = Path::new(dir_path).join(format!("{:10}.data", fid));
        Self {}
    }

    pub(crate) async fn read_log(&self, offset: usize) -> Result<Option<(LogRecord, usize)>> {
        unimplemented!()
    }

    pub(crate) async fn current_size(&self) -> Result<usize> {
        unimplemented!()
    }

    pub(crate) async fn fsync(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn fid(&self) -> Fid {
        unimplemented!()
    }
}
