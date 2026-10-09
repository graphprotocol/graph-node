use graph::futures01::sync::mpsc::Sender;
use graph::prelude::Error;
use std::collections::HashMap;
use std::sync::Arc;

/// Maps the hash of a module to a channel to the thread in which the module
/// is instantiated.
///
/// Hashing a module reads all of its bytes, and a subgraph can have millions
/// of data sources that share a handful of modules. The hash is therefore
/// memoized by the identity of the `Arc` that holds the bytes: every data
/// source created from the same manifest mapping or template shares that
/// mapping's `Arc`. The `Arc` is held by the memo, so its address can't be
/// reused for other bytes while the entry exists. Bytes in another `Arc` are
/// hashed again: equal contents give the same hash, hence the same module.
pub(super) struct ModuleCache<R> {
    senders: HashMap<[u8; 32], Sender<R>>,
    hashes: HashMap<usize, (Arc<Vec<u8>>, [u8; 32])>,
    #[cfg(test)]
    hashes_computed: usize,
}

impl<R> ModuleCache<R> {
    pub fn new() -> Self {
        ModuleCache {
            senders: HashMap::new(),
            hashes: HashMap::new(),
            #[cfg(test)]
            hashes_computed: 0,
        }
    }

    /// The keccak256 hash of `module_bytes`, computed once per `Arc`.
    fn hash(&mut self, module_bytes: &Arc<Vec<u8>>) -> [u8; 32] {
        let key = Arc::as_ptr(module_bytes) as usize;
        if let Some((bytes, hash)) = self.hashes.get(&key)
            && Arc::ptr_eq(bytes, module_bytes)
        {
            return *hash;
        }
        let hash = alloy::primitives::keccak256(module_bytes.as_slice()).0;
        #[cfg(test)]
        {
            self.hashes_computed += 1;
        }
        self.hashes.insert(key, (module_bytes.clone(), hash));
        hash
    }

    /// The sender to the module `module_bytes`, spawned by `spawn` if no
    /// module with the same hash was spawned before.
    pub fn sender(
        &mut self,
        module_bytes: &Arc<Vec<u8>>,
        spawn: impl FnOnce(&[u8]) -> Result<Sender<R>, Error>,
    ) -> Result<Sender<R>, Error> {
        let module_hash = self.hash(module_bytes);
        if let Some(sender) = self.senders.get(&module_hash) {
            return Ok(sender.clone());
        }
        let sender = spawn(module_bytes.as_slice())?;
        self.senders.insert(module_hash, sender.clone());
        Ok(sender)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph::futures01::sync::mpsc::{Receiver, channel};

    /// A cache whose spawns are recorded: the receivers, by spawn order.
    struct Spawns {
        cache: ModuleCache<u32>,
        receivers: Vec<Receiver<u32>>,
    }

    impl Spawns {
        fn new() -> Self {
            Spawns {
                cache: ModuleCache::new(),
                receivers: Vec::new(),
            }
        }

        fn sender(&mut self, bytes: &Arc<Vec<u8>>) -> Sender<u32> {
            let receivers = &mut self.receivers;
            self.cache
                .sender(bytes, |_| {
                    let (tx, rx) = channel(1);
                    receivers.push(rx);
                    Ok(tx)
                })
                .unwrap()
        }
    }

    /// Whether `sender` sends to the receiver of spawn `idx`: drop that
    /// receiver and see if the sender is disconnected.
    fn sends_to(mut spawns: Spawns, sender: &mut Sender<u32>, idx: usize) -> bool {
        drop(spawns.receivers.remove(idx));
        sender.try_send(0).is_err_and(|e| e.is_disconnected())
    }

    fn module(n: u8) -> Vec<u8> {
        (0..66_000u32).map(|i| (i as u8) ^ n).collect()
    }

    #[test]
    fn two_templates_two_hashes() {
        let a = Arc::new(module(1));
        let b = Arc::new(module(2));
        let mut s = Spawns::new();
        let ha = s.cache.hash(&a);
        let hb = s.cache.hash(&b);
        assert_ne!(ha, hb);
        assert_eq!(ha, alloy::primitives::keccak256(a.as_slice()).0);
        assert_eq!(hb, alloy::primitives::keccak256(b.as_slice()).0);

        s.sender(&a);
        let mut sb = s.sender(&b);
        assert_eq!(s.receivers.len(), 2, "one module per template");
        assert!(sends_to(s, &mut sb, 1));
    }

    #[test]
    fn sources_of_one_template_share_hash_and_sender() {
        let template = Arc::new(module(1));
        let other = Arc::new(module(2));
        let mut s = Spawns::new();
        s.sender(&other);
        s.sender(&template);
        let mut second = s.sender(&template.clone());
        assert_eq!(s.cache.hashes_computed, 2);
        assert_eq!(s.receivers.len(), 2);
        assert!(sends_to(s, &mut second, 1), "the template's sender");
    }

    #[test]
    fn many_sources_hash_once_per_template() {
        const N: usize = 100_000;
        let a = Arc::new(module(1));
        let b = Arc::new(module(2));
        let mut s = Spawns::new();
        for i in 0..N {
            let bytes = if i % 2 == 0 { a.clone() } else { b.clone() };
            s.sender(&bytes);
        }
        assert_eq!(s.cache.hashes_computed, 2);
        assert_eq!(s.receivers.len(), 2);

        let mut s = Spawns::new();
        for _ in 0..N {
            s.sender(&a.clone());
        }
        assert_eq!(s.cache.hashes_computed, 1);
        assert_eq!(s.receivers.len(), 1);
    }

    #[test]
    fn equal_unshared_bytes_give_same_hash_and_module() {
        let shared = Arc::new(module(1));
        let copy = Arc::new(module(1));
        assert!(!Arc::ptr_eq(&shared, &copy));
        let mut s = Spawns::new();
        s.sender(&Arc::new(module(2)));
        s.sender(&shared);
        let hash = s.cache.hash(&shared);
        assert_eq!(s.cache.hash(&copy), hash);
        assert_eq!(s.cache.hashes_computed, 3, "the copy is hashed");
        let mut from_copy = s.sender(&copy);
        assert_eq!(s.receivers.len(), 2, "no new module for the copy");
        assert!(sends_to(s, &mut from_copy, 1), "the shared bytes' sender");
    }

    #[test]
    fn dropped_bytes_are_not_confused_with_new_ones() {
        // Allocate and drop many `Arc`s: the memo holds each one, so no
        // later allocation can take the address of a memoized one.
        let mut s = Spawns::new();
        for n in 0..64u8 {
            let bytes = Arc::new(vec![n; 64]);
            assert_eq!(
                s.cache.hash(&bytes),
                alloy::primitives::keccak256(bytes.as_slice()).0
            );
        }
        assert_eq!(s.cache.hashes_computed, 64);
    }
}
