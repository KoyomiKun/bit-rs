use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    db::DB,
    index::{IndexIter, IndexOpt},
};

pub struct Iter<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIter>>>,
    db: &'a DB,
}

/// init iter
impl DB {
    pub fn iter(&self, opt: IndexOpt) -> Iter {
        Iter {
            index_iter: Arc::new(RwLock::new(self.index.iter(opt))),
            db: &self,
        }
    }
}
