use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use graph::{
    blockchain::Blockchain,
    cheap_clone::CheapClone,
    components::{
        store::BlockNumber,
        subgraph::{RuntimeHost, RuntimeHostBuilder},
    },
};

/// This structure maintains a partition of the hosts by address, for faster trigger matching. This
/// partition uses the host's index in the main vec, to maintain the correct ordering.
pub(super) struct OnchainHosts<C: Blockchain, T: RuntimeHostBuilder<C>> {
    hosts: Vec<Arc<T::Host>>,

    // The `usize` is the index of the host in `hosts`.
    hosts_by_address: HashMap<Box<[u8]>, Vec<usize>>,
    hosts_without_address: Vec<usize>,
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> OnchainHosts<C, T> {
    pub fn new() -> Self {
        Self {
            hosts: Vec::new(),
            hosts_by_address: HashMap::new(),
            hosts_without_address: Vec::new(),
        }
    }

    pub fn hosts(&self) -> &[Arc<T::Host>] {
        &self.hosts
    }

    pub fn contains(&self, other: &Arc<T::Host>) -> bool {
        // Narrow down the host list by address, as an optimization.
        let hosts = match other.data_source().address() {
            Some(address) => self.hosts_by_address.get(address.as_slice()),
            None => Some(&self.hosts_without_address),
        };

        hosts
            .into_iter()
            .flatten()
            .any(|idx| &self.hosts[*idx] == other)
    }

    pub fn last(&self) -> Option<&Arc<T::Host>> {
        self.hosts.last()
    }

    pub fn len(&self) -> usize {
        self.hosts.len()
    }

    pub fn push(&mut self, host: Arc<T::Host>) {
        assert!(host.data_source().as_onchain().is_some());

        self.hosts.push(host.cheap_clone());
        let idx = self.hosts.len() - 1;
        let address = host.data_source().address();
        match address {
            Some(address) => {
                self.hosts_by_address
                    .entry(address.into())
                    .or_default()
                    .push(idx);
            }
            None => {
                self.hosts_without_address.push(idx);
            }
        }
    }

    pub fn pop(&mut self) {
        let Some(host) = self.hosts.pop() else { return };
        let address = host.data_source().address();
        match address {
            Some(address) => {
                // Unwrap and assert: The same host we just popped must be the last one in `hosts_by_address`.
                let hosts = self.hosts_by_address.get_mut(address.as_slice()).unwrap();
                let idx = hosts.pop().unwrap();
                assert_eq!(idx, self.hosts.len());
            }
            None => {
                // Unwrap and assert: The same host we just popped must be the last one in `hosts_without_address`.
                let idx = self.hosts_without_address.pop().unwrap();
                assert_eq!(idx, self.hosts.len());
            }
        }
    }

    /// Returns an iterator over all hosts that match the given address, in the order they were inserted in `hosts`.
    /// Note that this always includes the hosts without an address, since they match all addresses.
    /// If no address is provided, returns an iterator over all hosts.
    pub fn matches_by_address(
        &self,
        address: Option<&[u8]>,
    ) -> Box<dyn Iterator<Item = &T::Host> + Send + '_> {
        let Some(address) = address else {
            return Box::new(self.hosts.iter().map(|host| host.as_ref()));
        };

        let mut matching_hosts: Vec<usize> = self
            .hosts_by_address
            .get(address)
            .into_iter()
            .flatten() // Flatten non-existing `address` into empty.
            .copied()
            .chain(self.hosts_without_address.iter().copied())
            .collect();
        matching_hosts.sort();
        Box::new(
            matching_hosts
                .into_iter()
                .map(move |idx| self.hosts[idx].as_ref()),
        )
    }
}

/// Note that unlike `OnchainHosts`, this does not maintain the order of insertion. Ultimately, the
/// processing order should not matter because each offchain ds has its own causality region.
pub(super) struct OffchainHosts<C: Blockchain, T: RuntimeHostBuilder<C>> {
    // Indexed by creation block
    by_block: BTreeMap<Option<BlockNumber>, Vec<Arc<T::Host>>>,
    // Indexed by `offchain::Source::address`
    by_address: BTreeMap<Vec<u8>, Vec<Arc<T::Host>>>,
    wildcard_address: Vec<Arc<T::Host>>,
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> OffchainHosts<C, T> {
    pub fn new() -> Self {
        Self {
            by_block: BTreeMap::new(),
            by_address: BTreeMap::new(),
            wildcard_address: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_block.values().map(Vec::len).sum()
    }

    pub fn all(&self) -> impl Iterator<Item = &Arc<T::Host>> + Send + '_ {
        self.by_block.values().flatten()
    }

    pub fn contains(&self, other: &Arc<T::Host>) -> bool {
        // Narrow down the host list by address, as an optimization.
        let hosts = match other.data_source().address() {
            Some(address) => self.by_address.get(address.as_slice()),
            None => Some(&self.wildcard_address),
        };

        hosts.into_iter().flatten().any(|host| host == other)
    }

    pub fn push(&mut self, host: Arc<T::Host>) {
        assert!(host.data_source().as_offchain().is_some());

        let block = host.creation_block_number();
        self.by_block
            .entry(block)
            .or_default()
            .push(host.cheap_clone());

        match host.data_source().address() {
            Some(address) => self.by_address.entry(address).or_default().push(host),
            None => self.wildcard_address.push(host),
        }
    }

    /// Removes all entries with block number >= block.
    pub fn remove_ge_block(&mut self, block: BlockNumber) {
        let removed = self.by_block.split_off(&Some(block));
        for (_, hosts) in removed {
            for host in hosts {
                match host.data_source().address() {
                    Some(address) => {
                        let hosts = self.by_address.get_mut(&address).unwrap();
                        hosts.retain(|h| !Arc::ptr_eq(h, &host));
                    }
                    None => {
                        self.wildcard_address.retain(|h| !Arc::ptr_eq(h, &host));
                    }
                }
            }
        }
    }

    pub fn matches_by_address<'a>(
        &'a self,
        address: Option<&[u8]>,
    ) -> Box<dyn Iterator<Item = &T::Host> + Send + 'a> {
        let Some(address) = address else {
            return Box::new(self.by_block.values().flatten().map(|host| host.as_ref()));
        };

        Box::new(
            self.by_address
                .get(address)
                .into_iter()
                .flatten() // Flatten non-existing `address` into empty.
                .map(|host| host.as_ref())
                .chain(self.wildcard_address.iter().map(|host| host.as_ref())),
        )
    }
}
