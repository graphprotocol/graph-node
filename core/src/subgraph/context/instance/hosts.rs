use std::{collections::HashMap, sync::Arc};

use graph::{
    blockchain::Blockchain,
    cheap_clone::CheapClone,
    components::subgraph::{RuntimeHost, RuntimeHostBuilder},
};

/// Runtime hosts, one for each data source mapping.
///
/// The runtime hosts are created and added to the vec in the same order the data sources appear in
/// the subgraph manifest. Incoming block stream events are processed by the mappings in this same
/// order.
///
/// This structure also maintains a partition of the hosts by address, for faster trigger matching.
/// This partition uses the host's index in the main vec, to maintain the correct ordering.
pub(super) struct Hosts<C: Blockchain, T: RuntimeHostBuilder<C>> {
    hosts: Vec<Arc<T::Host>>,

    // The `usize` is the index of the host in `hosts`.
    hosts_by_address: HashMap<Box<[u8]>, Vec<usize>>,
    hosts_without_address: Vec<usize>,
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> Hosts<C, T> {
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
        self.hosts.contains(other)
    }

    pub fn last(&self) -> Option<&Arc<T::Host>> {
        self.hosts.last()
    }

    pub fn len(&self) -> usize {
        self.hosts.len()
    }

    pub fn push(&mut self, host: Arc<T::Host>) {
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
    pub fn iter_by_address(
        &self,
        address: Option<Vec<u8>>,
    ) -> Box<dyn Iterator<Item = &T::Host> + Send + '_> {
        let Some(address) = address else {
            return Box::new(self.hosts.iter().map(|host| host.as_ref()));
        };

        let mut matching_hosts: Vec<usize> = self
            .hosts_by_address
            .get(address.as_slice())
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
