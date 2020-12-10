use std::collections::HashMap;
mod sled;
pub use self::sled::SledStore;

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

    fn tip(&self) -> Result<Option<HeaderView>, Error> {
        Ok(None)
    }
    fn get_header(&self, block_hash: packed::Byte32) -> Result<Option<HeaderView>, Error> {
        Ok(None)
    }
    fn get_total_difficulty(&self, block_hash: packed::Byte32) -> Result<Option<U256>, Error> {
        Ok(None)
    }
    fn get_block_hash(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<packed::Byte32>, Error> {
        Ok(None)
    }

    fn init(&self, genesis: HeaderView) -> Result<(), Error> {
        Ok(())
    }

    fn insert_gcsfilter(&self, filter: packed::GcsFilter)->Result<(), Error> {
        Ok(())
    }

    fn get_gcsfilter(&self, block_hash: packed::Byte32) -> Result<Option<packed::Bytes>, Error> {
        Ok(None)
    }

    fn get_lastest_hash(&self)->Result<Option<packed::Byte32>, Error> {
        Ok(None)
    }

    fn get_lastest_block_num(&self)->Result<Option<BlockNumber>, Error> {
        Ok(None)
    }

    fn insert_header(&self, header: HeaderView) -> Result<(), Error> {
        Ok(())
    }

    fn get_locator(&self) -> Result<Vec<packed::Byte32>, Error> {
        Ok(Vec::new())
    }

    fn insert_filtered_block(
        &self,
        block: BlockView
    ) -> Result<(), Error> {
        Ok(())
    }

    fn get_out_point(
        &self,
        out_point: packed::OutPoint,
    ) -> Result<Option<(packed::CellOutput, packed::Bytes, BlockNumber)>, Error> {
        Ok(None)
    }

    fn get_consumed_out_point(
        &self,
        out_point: packed::OutPoint,
    ) -> Result<Option<(packed::CellOutput, packed::Bytes, BlockNumber)>, Error> {
        Ok(None)
    }

    fn insert_script(
        &self,
        script: packed::Script,
        block_number: BlockNumber,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn update_scripts(&mut self, last_number: BlockNumber) -> Result<(), Error> {
        Ok(())
    }
    fn get_cells(
        &self,
        script: &packed::Script,
    ) -> Result<
        Vec<(
            packed::OutPoint,
            packed::CellOutput,
            packed::Bytes,
            BlockNumber,
        )>,
        Error> {
        Ok(Vec::new())
    }
    
    fn get_consumed_cells(
        &self,
        script: &packed::Script,
    ) -> Result<
        Vec<(
            packed::OutPoint,
            packed::CellOutput,
            packed::Bytes,
            BlockNumber,
            packed::Byte32,
            BlockNumber,
        )>,
        Error> {
        Ok(Vec::new())
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
