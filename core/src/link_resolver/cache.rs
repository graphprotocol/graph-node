use graph::prelude::CheapClone;
use mini_moka::sync::Cache as MiniMoka;

#[derive(Clone)]
pub struct LinkResolverCache {
    moka: MiniMoka<String, Vec<u8>>,
}

impl LinkResolverCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            moka: MiniMoka::new(capacity as u64),
        }
    }

    pub fn get(&self, key: impl ToString) -> Option<Vec<u8>> {
        self.moka.get(&key.to_string()).map(|compressed| {
            zstd::stream::decode_all(&compressed[..]).expect("Unexpected decompression error")
        })
    }

    pub fn contains(&self, key: impl ToString) -> bool {
        self.moka.contains_key(&key.to_string())
    }

    pub fn insert(&self, key: String, value: Vec<u8>) {
        let compressed = zstd::stream::encode_all(&value[..], zstd::DEFAULT_COMPRESSION_LEVEL)
            .expect("Can't fail to Read from Vec<u8>");
        self.moka.insert(key, compressed);
    }
}

impl CheapClone for LinkResolverCache {}
