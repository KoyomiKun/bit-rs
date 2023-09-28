use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use bytes::Bytes;

use crate::index::Index;
use crate::meta::LogPos;

use super::{IndexIter, IndexOpt};

pub(crate) struct BTreeIndex {
    tree: Arc<RwLock<BTreeMap<Bytes, LogPos>>>,
}

impl Index for BTreeIndex {
    fn put(&mut self, key: Bytes, pos: LogPos) -> Result<()> {
        let mut g = self.tree.write().unwrap();
        g.insert(key, pos);
        Ok(())
    }

    fn get(&self, key: &Bytes) -> Option<LogPos> {
        let r = self.tree.read().unwrap();
        r.get(key).copied()
    }

    fn delete(&mut self, key: &Bytes) -> Result<()> {
        let mut g = self.tree.write().unwrap();
        g.remove(key);
        Ok(())
    }

    fn iter(&self, opt: IndexOpt) -> Box<dyn IndexIter> {
        Box::new(BTreeIndexIter::new(self, opt))
    }
}

impl BTreeIndex {
    pub(crate) fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

// TODO: now it creates a snapshot on current BTree, which waste memory and need beeing optimized
struct BTreeIndexIter {
    v: Vec<(Bytes, LogPos)>,
    pos: usize,
    opt: IndexOpt,
}

impl BTreeIndexIter {
    pub(crate) fn new(idx: &BTreeIndex, opt: IndexOpt) -> Self {
        let g = idx.tree.read().unwrap();
        let mut v = Vec::with_capacity(g.len());
        if !opt.reverse {
            for (i, (b, p)) in g.iter().enumerate() {
                v.insert(i, (b.clone(), p.clone())) // clone is not expensive
            }
        } else {
            for (i, (b, p)) in g.iter().rev().enumerate() {
                v.insert(i, (b.clone(), p.clone())) // clone is not expensive
            }
        }
        Self { v, pos: 0, opt }
    }
}

impl<'a> IndexIter for BTreeIndexIter {
    fn next(&mut self) -> Option<(Bytes, LogPos)> {
        while let Some(v) = self.v.get(self.pos) {
            self.pos += 1;
            if self.opt.prefix.is_empty() || v.0.starts_with(&self.opt.prefix) {
                return Some(v.to_owned());
            }
        }
        None
    }

    // find the first pos which value is larger than key
    fn seek(&mut self, key: Bytes) {
        self.pos = match self.v.binary_search_by(|(x, _)| {
            if self.opt.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Err(e) => e,
            Ok(v) => v,
        };
    }

    fn rewind(&mut self) {
        self.pos = 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_index() {
        let mut i = BTreeIndex::new();

        let key = Bytes::from_static(&[10]);
        i.put(key, LogPos { fid: 1, offset: 2 }).unwrap();
        assert_eq!(i.get(&key), Some(LogPos { fid: 1, offset: 2 }));

        i.delete(&key).unwrap();
        assert_eq!(i.get(&key), None);
    }
}
