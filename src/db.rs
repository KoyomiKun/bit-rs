use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Ok, Result};
use tokio::fs::read_dir;

use crate::{
    disk::data_file::DataFile,
    index::{btree::BTreeIndex, Index},
    meta::{Fid, LogPos},
};

pub enum IndexerType {
    BTreeMap,
}

pub struct Options {
    dir_path: String,
    max_file_size: usize,
    index_type: IndexerType,
    sync_write: bool,
}

impl Options {
    fn validate(&self) -> bool {
        unimplemented!()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: String::new(),
            max_file_size: 1 * 1024 * 1024 * 1024,
            index_type: IndexerType::BTreeMap,
            sync_write: false,
        }
    }
}

pub struct DB {
    sync_write: bool,
    max_file_size: usize,
    dir_path: String,

    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<Fid, DataFile>>>,
    index: Box<dyn Index>,
}

impl DB {
    pub async fn new(opts: Options) -> Result<DB> {
        if !opts.validate() {
            return Err(anyhow!("invalid options"));
        }
        // read data file
        let (af, ofs, idx) = Self::load_data_files(&opts.dir_path, &opts.index_type)
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

    async fn load_data_files(
        dir_path: &str,
        index_type: &IndexerType,
    ) -> Result<(DataFile, HashMap<Fid, DataFile>, Box<dyn Index>)> {
        let mut dir = read_dir(dir_path).await?;
        let mut dfv = Vec::new();

        while let Some(e) = dir.next_entry().await? {
            let filename = e.file_name();
            let filename = filename.to_str().expect("unexpected filename");
            if !filename.ends_with(".data") {
                continue;
            }
            let file_index = filename.trim_end_matches(".data").parse::<Fid>()?;
            dfv.push((file_index, DataFile::new(dir_path, file_index)));
        }

        // init empty db
        if dfv.is_empty() {
            dfv.push((0, DataFile::new(dir_path, 0)))
        }

        // read index
        dfv.sort_by_key(|(fid, d)| *fid);
        let mut idx = Self::index_type_to_indexer(index_type);

        for (fid, d) in &dfv {
            let mut offset = 0;

            while let Some((record, size)) = d.read_log(offset).await? {
                idx.put(record.key.to_vec(), LogPos { fid: *fid, offset })?;
                offset += size;
            }
        }

        let active_file = dfv.pop().unwrap().1;

        let old_map = dfv.into_iter().collect::<HashMap<Fid, DataFile>>();
        return Ok((active_file, old_map, idx));
    }

    async fn append_log_record(&self) -> Result<()> {
        let mut f = self.active_file.write().unwrap();
        if f.current_size().await? > self.max_file_size {
            f.fsync().await?;
            *f = DataFile::new(self.dir_path.as_str(), f.fid() + 1);
        }
        Ok(())
    }

    fn index_type_to_indexer(it: &IndexerType) -> Box<dyn Index> {
        match it {
            IndexerType::BTreeMap => Box::new(BTreeIndex::new()),
            _ => panic!("BUG: invalid indexer pass validation"),
        }
    }
}
