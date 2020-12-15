use std::collections::HashMap;
use sled::{open, Db};
use std::sync::Arc;
use ckb_types::{
    core::{BlockNumber, HeaderView, BlockView},
    packed,
    U256,
};


#[derive(Debug)]
pub enum Error {
    DBError(String),
}

impl ToString for Error {
    fn to_string(&self) -> String {
        match self {
            Error::DBError(err) => err.to_owned(),
        }
    }
}

pub type IteratorItem = (Vec<u8>, Vec<u8>);

pub enum IteratorDirection {
    Forward,
    Reverse,
}

pub trait Store {
    type Batch: Batch;
    fn new(path: &str) -> Self;
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error>;
    fn exists<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, Error>;
    fn iter<K: AsRef<[u8]>>(
        &self,
        from_key: K,
        direction: IteratorDirection,
    ) -> Result<Box<dyn Iterator<Item = IteratorItem>>, Error>;
    fn batch(&self) -> Result<Self::Batch, Error>;

    // returns key_prefix => (entries, totol size, total value size)
    fn statistics(&self) -> Result<HashMap<u8, (usize, usize, usize)>, Error> {
        let iter = self.iter(&[], IteratorDirection::Forward)?;
        let mut statistics: HashMap<u8, (usize, usize, usize)> = HashMap::new();
        for (key, value) in iter {
            let s = statistics.entry(*key.first().unwrap()).or_default();
            s.0 += 1;
            s.1 += key.len();
            s.2 += value.len();
        }
        Ok(statistics)
    }
}

pub trait Batch {
    fn put_kv<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), Error> {
        self.put(&Into::<Vec<u8>>::into(key), &Into::<Vec<u8>>::into(value))
    }

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<(), Error>;
    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), Error>;
    fn commit(self) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct SledStore {
    db: Arc<Db>,
}

impl Store for SledStore {
    type Batch = SledBatch;

    fn new(path: &str) -> Self {
        let db = Arc::new(open(path).expect("Failed to open sled"));
        Self { db }
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        self.db
            .get(key.as_ref())
            .map(|v| v.map(|vi| vi.to_vec()))
            .map_err(Into::into)
    }

    fn exists<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, Error> {
        self.db
            .get(key.as_ref())
            .map(|v| v.is_some())
            .map_err(Into::into)
    }

    fn iter<K: AsRef<[u8]>>(
        &self,
        from_key: K,
        mode: IteratorDirection,
    ) -> Result<Box<dyn Iterator<Item = IteratorItem>>, Error> {
        match mode {
            IteratorDirection::Forward => {
                let iter = self.db.range(from_key.as_ref()..).map(|i| {
                    let (key, value) = i.unwrap();
                    (Vec::from(key.as_ref()), Vec::from(value.as_ref()))
                });
                Ok(Box::new(iter) as Box<_>)
            }
            IteratorDirection::Reverse => {
                let iter = self.db.range(..=from_key.as_ref()).map(|i| {
                    let (key, value) = i.unwrap();
                    (Vec::from(key.as_ref()), Vec::from(value.as_ref()))
                });
                Ok(Box::new(iter.rev()) as Box<_>)
            }
        }
    }

    fn batch(&self) -> Result<Self::Batch, Error> {
        Ok(Self::Batch {
            db: Arc::clone(&self.db),
            batch: sled::Batch::default(),
        })
    }
}

pub struct SledBatch {
    db: Arc<Db>,
    batch: sled::Batch,
}

impl Batch for SledBatch {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<(), Error> {
        self.batch.insert(key.as_ref(), value.as_ref());
        Ok(())
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), Error> {
        self.batch.remove(key.as_ref());
        Ok(())
    }

    fn commit(self) -> Result<(), Error> {
        self.db.apply_batch(self.batch)?;
        Ok(())
    }
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Error {
        Error::DBError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn put_and_get() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("put_and_get")
            .tempdir()
            .unwrap();
        let store = SledStore::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.put(&[1, 1], &[1, 1, 1]).unwrap();
        batch.commit().unwrap();

        assert_eq!(Some(vec![0, 0, 0]), store.get(&[0, 0]).unwrap());
        assert_eq!(Some(vec![1, 1, 1]), store.get(&[1, 1]).unwrap());
        assert_eq!(None, store.get(&[2, 2]).unwrap());
    }

    #[test]
    fn exists() {
        let tmp_dir = tempfile::Builder::new().prefix("exists").tempdir().unwrap();
        let store = SledStore::new(tmp_dir.path().to_str().unwrap());
        assert!(!store.exists(&[0, 0]).unwrap());

        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.commit().unwrap();

        assert!(store.exists(&[0, 0]).unwrap());
    }

    #[test]
    fn delete() {
        let tmp_dir = tempfile::Builder::new().prefix("delete").tempdir().unwrap();
        let store = SledStore::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.commit().unwrap();
        assert_eq!(Some(vec![0, 0, 0]), store.get(&[0, 0]).unwrap());

        let mut batch = store.batch().unwrap();
        batch.delete(&[0, 0]).unwrap();
        batch.commit().unwrap();
        assert_eq!(None, store.get(&[0, 0]).unwrap());
    }

    #[test]
    fn iter() {
        let tmp_dir = tempfile::Builder::new().prefix("iter").tempdir().unwrap();
        let store = SledStore::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0, 0], &[0, 0, 0]).unwrap();
        batch.put(&[0, 0, 1], &[0, 0, 1]).unwrap();
        batch.put(&[1, 0, 0], &[1, 0, 0]).unwrap();
        batch.put(&[1, 0, 1], &[1, 0, 1]).unwrap();
        batch.commit().unwrap();

        let mut iter = store.iter(&[0, 0, 1], IteratorDirection::Forward).unwrap();
        assert_eq!(
            Some((vec![0, 0, 1], vec![0, 0, 1])),
            iter.next().map(|i| (i.0.to_vec(), i.1.to_vec()))
        );
        assert_eq!(
            Some((vec![1, 0, 0], vec![1, 0, 0])),
            iter.next().map(|i| (i.0.to_vec(), i.1.to_vec()))
        );

        let mut iter = store.iter(&[0, 0, 1], IteratorDirection::Reverse).unwrap();
        assert_eq!(
            Some((vec![0, 0, 1], vec![0, 0, 1])),
            iter.next().map(|i| (i.0.to_vec(), i.1.to_vec()))
        );
        assert_eq!(
            Some((vec![0, 0, 0], vec![0, 0, 0])),
            iter.next().map(|i| (i.0.to_vec(), i.1.to_vec()))
        );
        assert!(iter.next().is_none());
    }
}
