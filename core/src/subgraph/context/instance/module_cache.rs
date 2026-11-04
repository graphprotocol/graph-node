use graph::futures01::sync::mpsc::Sender;
use std::collections::HashMap;
use std::sync::Arc;

/// Maps the hash of a module to a channel to the thread in which the module
/// is instantiated.
///
/// Hashing a module reads all of its bytes, and a subgraph can have millions
/// of data sources that share a handful of modules. The hash is therefore
/// memoized by the identity of the `Arc` that holds the bytes: every data
/// source created from the same manifest mapping or template shares that
/// mapping's `Arc`. The memo holds a clone of the `Arc`, so the allocation
/// lives as long as the entry and an address found in the memo always
/// designates that same `Arc`. Bytes in another `Arc` are hashed again: equal
/// contents give the same hash, hence the same module.
pub(super) struct ModuleCache<R> {
    senders: HashMap<[u8; 32], Sender<R>>,
    hashes: HashMap<usize, (Arc<Vec<u8>>, [u8; 32])>,
}

impl<R> ModuleCache<R> {
    pub fn new() -> Self {
        ModuleCache {
            senders: HashMap::new(),
            hashes: HashMap::new(),
        }
    }

    /// The keccak256 hash of `module_bytes`, computed once per `Arc`.
    pub fn hash(&mut self, module_bytes: &Arc<Vec<u8>>) -> [u8; 32] {
        let key = Arc::as_ptr(module_bytes) as usize;
        if let Some((_, hash)) = self.hashes.get(&key) {
            return *hash;
        }
        let hash = alloy::primitives::keccak256(module_bytes.as_slice()).0;
        self.hashes.insert(key, (module_bytes.clone(), hash));
        hash
    }

    pub fn get(&self, module_hash: &[u8; 32]) -> Option<&Sender<R>> {
        self.senders.get(module_hash)
    }

    pub fn insert(&mut self, module_hash: [u8; 32], sender: Sender<R>) {
        self.senders.insert(module_hash, sender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph::futures01::sync::mpsc::{Receiver, channel};

    /// A cache used like `new_host` uses it, whose spawns are recorded: the
    /// receivers, by spawn order.
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

        /// Memoize `mark` as the hash of `bytes`.
        fn seed(&mut self, bytes: &Arc<Vec<u8>>, mark: [u8; 32]) {
            let key = Arc::as_ptr(bytes) as usize;
            self.cache.hashes.insert(key, (bytes.clone(), mark));
        }

        fn sender(&mut self, bytes: &Arc<Vec<u8>>) -> Sender<u32> {
            let module_hash = self.cache.hash(bytes);
            if let Some(sender) = self.cache.get(&module_hash) {
                return sender.clone();
            }
            let (tx, rx) = channel(1);
            self.receivers.push(rx);
            self.cache.insert(module_hash, tx.clone());
            tx
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
        assert_eq!(s.cache.hashes.len(), 2);
        assert_eq!(s.receivers.len(), 2);
        assert!(sends_to(s, &mut second, 1), "the template's sender");
    }

    #[test]
    fn many_sources_hash_once_per_template() {
        const N: usize = 100_000;
        // A recomputed hash overwrites its entry, which `hashes.len()` can't
        // show. Marks are not real hashes on purpose: only a hit returns them.
        let (mark_a, mark_b) = ([0xaa; 32], [0xbb; 32]);
        let a = Arc::new(module(1));
        let b = Arc::new(module(2));
        let mut s = Spawns::new();
        s.seed(&a, mark_a);
        s.seed(&b, mark_b);
        for i in 0..N {
            let (bytes, mark) = if i % 2 == 0 {
                (a.clone(), mark_a)
            } else {
                (b.clone(), mark_b)
            };
            assert_eq!(s.cache.hash(&bytes), mark);
            s.sender(&bytes);
        }
        assert_eq!(s.cache.hashes.len(), 2);
        assert_eq!(s.receivers.len(), 2);

        let mut s = Spawns::new();
        for _ in 0..N {
            s.sender(&a.clone());
        }
        assert_eq!(s.cache.hashes.len(), 1);
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
        assert_eq!(s.cache.hashes.len(), 3, "the copy is hashed");
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
        assert_eq!(s.cache.hashes.len(), 64);
    }
}
