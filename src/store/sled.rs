use super::{Batch, Error, IteratorDirection, IteratorItem, Store};
use sled::{open, Db};
use std::sync::Arc;

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
