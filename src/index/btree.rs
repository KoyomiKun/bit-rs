use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use bytes::Bytes;

use crate::index::Index;
use crate::meta::LogPos;

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
}

impl BTreeIndex {
    pub(crate) fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Iterator for BTreeIndex {
    type Item = &LogPos;
    fn next(&mut self) -> Option<Self::Item> {
        let r = self.tree.read().unwrap();
        r.iter().next().map(|(_, v)| v)
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
