use std::path::Path;

use anyhow::Result;

use crate::meta::LogRecord;

pub(crate) struct DataFile {}

impl DataFile {
    pub(crate) fn new(path: &Path) -> Self {
        Self {}
    }

    pub(crate) async fn read_log(&self, offset: usize) -> Result<Option<(LogRecord, usize)>> {
        unimplemented!()
    }
}
