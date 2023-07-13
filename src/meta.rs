#[derive(Debug)]
pub(crate) struct LogPos {
    pub(crate) fid: u32,
    pub(crate) offset: usize,
}

pub(crate) enum RecordType {
    Delete,
    Normal,
}

// log entry
pub(crate) struct LogRecord<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
    pub(crate) typ: RecordType,
}
