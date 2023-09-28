use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Result};
use tokio::fs;

use bytes::Bytes;

use crate::{
    disk::data_file::DataFile,
    index::{btree::BTreeIndex, Index},
    meta::{Fid, LogPos, LogRecord, RecordType},
};

pub enum IndexerType {
    BTreeMap,
}

pub struct Options<'a> {
    dir_path: &'a Path,
    max_file_size: usize,
    index_type: IndexerType,
    sync_write: bool,
}

impl<'a> Options<'a> {
    fn validate(&self) -> bool {
        let s = self.dir_path.to_str();
        if s.is_none() {
            return false;
        }
        if s.unwrap().len() == 0 {
            return false;
        }

        if self.max_file_size == 0 {
            return false;
        }

        true
    }
}

impl<'a> Default for Options<'a> {
    fn default() -> Self {
        Self {
            dir_path: Path::new(""),
            max_file_size: 1 * 1024 * 1024 * 1024,
            index_type: IndexerType::BTreeMap,
            sync_write: false,
        }
    }
}

pub struct DB {
    sync_write: bool,
    max_file_size: usize,
    dir_path: PathBuf,

    // keep outter ops exclusive
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<Fid, DataFile>>>,
    pub(crate) index: Box<dyn Index>,
}

impl<'a> DB<'a> {
    pub async fn open(opts: Options<'a>) -> Result<DB> {
        if !opts.validate() {
            return Err(anyhow!("invalid options"));
        }
        // read data file
        let (af, ofs, idx) = Self::load_data_files(opts.dir_path, &opts.index_type)
            .await
            .map_err(|e| anyhow!("load data files failed: {e}"))?;
        return Ok(DB {
            sync_write: opts.sync_write,
            max_file_size: opts.max_file_size,
            dir_path: opts.dir_path,
            active_file: Arc::new(RwLock::new(af)),
            older_files: Arc::new(RwLock::new(ofs)),
            index: idx,
        });
    }

    pub async fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(anyhow!("empty key is invalid"));
        }

        let l = LogRecord {
            key,
            value,
            typ: RecordType::Normal,
        };

        self.append_log_record(l).await
    }

    pub async fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        if key.is_empty() {
            return Err(anyhow!("empty key is invalid"));
        }
        if let Some(lr) = self
            .get_record(key)
            .await
            .map_err(|e| anyhow!("get record failed: {e}"))?
        {
            if lr.typ == RecordType::Normal {
                return Ok(Some(lr.value.into()));
            }
        }
        Ok(None)
    }

    pub async fn delete(&mut self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(anyhow!("empty key is invalid"));
        }

        if self.index.get(&key).is_none() {
            return Ok(());
        }

        let l = LogRecord {
            key,
            value: Bytes::default(),
            typ: RecordType::Delete,
        };

        self.append_log_record(l).await
    }

    async fn load_data_files(
        dir_path: &Path,
        index_type: &IndexerType,
    ) -> Result<(DataFile, HashMap<Fid, DataFile>, Box<dyn Index>)> {
        if !dir_path.exists() || !dir_path.is_dir() {
            fs::create_dir_all(dir_path)
                .await
                .map_err(|e| anyhow!("create dir failed: {e}"))?;
        }
        let mut dir = fs::read_dir(dir_path)
            .await
            .map_err(|e| anyhow!("read dir failed: {e}"))?;

        let mut dfv = Vec::new();

        while let Some(e) = dir.next_entry().await? {
            let filename = e.file_name();
            let filename = filename.to_str().expect("unexpected filename");
            if !filename.ends_with(".data") {
                continue;
            }
            let file_index = filename
                .trim_end_matches(".data")
                .parse::<Fid>()
                .map_err(|e| anyhow!("parse data file name failed: {e}"))?;
            dfv.push((
                file_index,
                DataFile::new(dir_path, file_index)
                    .map_err(|e| anyhow!("create datafile {file_index} failed: {e}"))?,
            ));
        }

        // init empty db
        if dfv.is_empty() {
            dfv.push((
                0,
                DataFile::new(dir_path, 0)
                    .map_err(|e| anyhow!("create empty datafile failed: {e}"))?,
            ))
        }
        dfv.sort_by_key(|(fid, _)| *fid);

        // read index

        let mut idx = Self::index_type_to_indexer(index_type);

        for (fid, d) in &dfv {
            let mut offset = 0;
            let (record, size) = d.read_record(offset).await?;
            match record.typ {
                RecordType::Normal => {
                    idx.put(record.key, LogPos { fid: *fid, offset })
                        .map_err(|e| anyhow!("save index record failed: {e}"))?;
                }
                RecordType::Delete => {
                    idx.delete(&record.key)
                        .map_err(|e| anyhow!("delete index record failed: {e}"))?;
                }
            }
            offset += size;
        }

        let active_file = dfv.pop().unwrap().1;
        let old_map = dfv.into_iter().collect::<HashMap<Fid, DataFile>>();
        return Ok((active_file, old_map, idx));
    }

    async fn append_log_record(&mut self, lr: LogRecord) -> Result<()> {
        if lr.typ == RecordType::Delete && self.index.get(&lr.key).is_none() {
            return Ok(());
        }

        let mut f = self.active_file.write().unwrap();
        // if current active file is over size, fsync and create a new active file
        // move the old one to old_files
        let offset = f.current_size().await;
        let serded_len = lr.serde_len();
        if offset + serded_len > self.max_file_size {
            f.fsync().await?;
            {
                let mut g = self.older_files.write().unwrap();
                g.insert(
                    f.fid(),
                    DataFile::new(self.dir_path, f.fid())
                        .map_err(|e| anyhow!("create datafile failed: {e}"))?,
                );
            }
            let nf = DataFile::new(self.dir_path, f.fid() + 1)
                .map_err(|e| anyhow!("create datafile failed: {e}"))?;
            *f = nf;
        }

        f.append_record(&lr)
            .await
            .map_err(|e| anyhow!("append entry to disk failed: {e}"))?;

        if self.sync_write {
            f.fsync().await?;
        }

        match lr.typ {
            RecordType::Delete => self
                .index
                .delete(&lr.key)
                .map_err(|e| anyhow!("delete index failed: {e}")),
            RecordType::Normal => {
                // write index
                self.index
                    .put(
                        lr.key,
                        LogPos {
                            fid: f.fid(),
                            offset,
                        },
                    )
                    .map_err(|e| anyhow!("put index failed: {e}"))
            }
        }
    }

    async fn get_record(&self, key: Bytes) -> Result<Option<LogRecord>> {
        if key.is_empty() {
            return Err(anyhow!("empty key is invalid"));
        }

        if let Some(idx) = self.index.get(&key) {
            {
                let af = self.active_file.read().unwrap();
                if af.fid() == idx.fid {
                    return Ok(Some(
                        af.read_record(idx.offset)
                            .await
                            .map_err(|e| {
                                anyhow!("read bytes from file ID {} failed: {e}", idx.fid)
                            })?
                            .0,
                    ));
                }
            }

            let of = self.older_files.read().unwrap();
            if let Some(df) = of.get(&idx.fid) {
                return Ok(Some(
                    df.read_record(idx.offset)
                        .await
                        .map_err(|e| anyhow!("read bytes from file ID {} failed: {e}", idx.fid))?
                        .0,
                ));
            }

            panic!("BUG: inconsistent index, data file lack {:?}", idx);
        }
        Ok(None)
    }

    fn index_type_to_indexer(it: &IndexerType) -> Box<dyn Index> {
        match it {
            IndexerType::BTreeMap => Box::new(BTreeIndex::new()),
            _ => panic!("BUG: invalid indexer pass validation"),
        }
    }
}
