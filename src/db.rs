use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Ok, Result};
use tokio::fs::read_dir;

use crate::{
    disk::data_file::DataFile,
    index::{BTreeIndexer, Indexer},
    meta::LogPos,
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
    opts: Options,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    index: Box<dyn Indexer>,
}

impl DB {
    pub async fn new(opts: Options) -> Result<DB> {
        if !opts.validate() {
            return Err(anyhow!("invalid options"));
        }
        // read data file
        if let Some((af, ofs)) = Self::load_data_files(&opts.dir_path)
            .await
            .map_err(|e| anyhow!("load data files failed: {e}"))?
        {
            let mut ordered_keys = ofs.keys().collect::<Vec<&u32>>();
            ordered_keys.sort();
            let index = Self::load_index_from_data(ordered_keys, &ofs, &af, &opts.index_type)
                .await
                .map_err(|e| anyhow!("load index from data failed: {e}"))?;
            // read index from data files
            Ok(Self {
                opts,
                active_file: Arc::new(RwLock::new(af)),
                older_files: Arc::new(RwLock::new(ofs)),
                index,
            })
        } else {
            // if data dir is empty, init db
            unimplemented!()
        }
    }

    async fn load_data_files(dir_path: &str) -> Result<Option<Vec>> {
        let mut dir = read_dir(dir_path).await?;
        let mut hm = HashMap::new();
        let mut max_idx = None;
        while let Some(e) = dir.next_entry().await? {
            let filename = e.file_name();
            let filename = filename.to_str().expect("unexpected filename");
            if !filename.ends_with(".data") {
                continue;
            }
            let file_index = filename.trim_end_matches(".data").parse::<u32>()?;
            hm.insert(
                file_index,
                DataFile::new(&Path::new(dir_path).join(e.file_name())),
            );

            if max_idx.is_none() {
                max_idx = Some(file_index);
                continue;
            }

            if let Some(lm) = max_idx {
                if lm < file_index {
                    max_idx = Some(file_index)
                }
            }
        }

        if max_idx.is_none() {
            return Ok(None);
        }
        Ok(Some((
            hm.remove(&max_idx.unwrap())
                .expect("BUG: max_idx not in hashmap"),
            hm,
        )))
    }

    async fn load_index_from_data(
        ordered_keys: Vec<&u32>,
        ofs: &HashMap<u32, DataFile>,
        af: &DataFile,
        index_type: &IndexerType,
    ) -> Result<Box<dyn Indexer>> {
        // read data from old to new
        let mut idx = Self::index_type_to_indexer(index_type);

        for fk in ordered_keys {
            let v = ofs
                .get(fk)
                .expect("BUG: get non-existed key from old files");
            let mut offset = 0;

            while let Some((record, size)) = v.read_log(offset).await? {
                idx.put(record.key.to_vec(), LogPos { fid: *fk, offset })?;
                offset += size;
            }
        }

        // read active file
        unimplemented!()
    }

    fn index_type_to_indexer(it: &IndexerType) -> Box<dyn Indexer> {
        match it {
            IndexerType::BTreeMap => Box::new(BTreeIndexer::new()),
            _ => panic!("BUG: invalid indexer pass validation"),
        }
    }
}
