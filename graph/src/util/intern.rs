//! Interning of strings.
//!
//! This module provides an interned string pool `AtomPool` and a map-like
//! data structure `Object` that uses the string pool. It offers two
//! different kinds of atom: a plain `Atom` (an integer) and a `FatAtom` (a
//! reference to the pool and an integer). The former is useful when the
//! pool is known from context whereas the latter carries a reference to the
//! pool and can be used anywhere.

use std::convert::TryFrom;
use std::{collections::HashMap, sync::Arc};

use serde::Serialize;

use crate::cheap_clone::CheapClone;
use crate::data::value::Word;
use crate::derive::CheapClone;
use crate::runtime::gas::{Gas, GasSizeOf};

use super::cache_weight::CacheWeight;

// An `Atom` is really just an integer value of this type. The size of the
// type determines how many atoms a pool (and all its parents) can hold.
type AtomInt = u16;

/// An atom in a pool. To look up the underlying string, surrounding code
/// needs to know the pool for it.
///
/// The ordering for atoms is based on their integer value, and has no
/// connection to how the strings they represent would be ordered
#[derive(Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Copy, CheapClone, Debug)]
pub struct Atom(AtomInt);

impl Atom {
    /// The underlying integer value of this atom, suitable for use as a Vec index.
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

/// An atom and the underlying pool. A `FatAtom` can be used in place of a
/// `String` or `Word`
#[allow(dead_code)]
pub struct FatAtom {
    pool: Arc<AtomPool>,
    atom: Atom,
}

impl FatAtom {
    pub fn as_str(&self) -> &str {
        self.pool.get(self.atom).expect("atom is in the pool")
    }
}

impl AsRef<str> for FatAtom {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Debug)]
pub enum Error {
    NotInterned(String),
}

impl Error {
    pub fn not_interned(self) -> String {
        match self {
            Error::NotInterned(s) => s,
        }
    }
}

#[derive(Debug, PartialEq, Default)]
/// A pool of interned strings. Pools can be organized hierarchically with
/// lookups in child pools also considering the parent pool. The chain of
/// pools from a pool through all its ancestors act as one big pool to the
/// outside.
pub struct AtomPool {
    base: Option<Arc<AtomPool>>,
    base_sym: AtomInt,
    atoms: Vec<Box<str>>,
    words: HashMap<Box<str>, Atom>,
}

impl AtomPool {
    /// Create a new root pool.
    pub fn new() -> Self {
        Self {
            base: None,
            base_sym: 0,
            atoms: Vec::new(),
            words: HashMap::new(),
        }
    }

    /// Create a child pool that extends the set of strings interned in the
    /// current pool.
    pub fn child(self: &Arc<Self>) -> Self {
        let base_sym = AtomInt::try_from(self.atoms.len()).unwrap();
        AtomPool {
            base: Some(self.clone()),
            base_sym,
            atoms: Vec::new(),
            words: HashMap::new(),
        }
    }

    /// Get the string for `atom`. Return `None` if the atom is not in this
    /// pool or any of its ancestors.
    pub fn get(&self, atom: Atom) -> Option<&str> {
        if atom.0 < self.base_sym {
            self.base.as_ref().and_then(|base| base.get(atom))
        } else {
            self.atoms
                .get((atom.0 - self.base_sym) as usize)
                .map(|s| s.as_ref())
        }
    }

    /// Get the atom for `word`. Return `None` if the word is not in this
    /// pool or any of its ancestors.
    pub fn lookup(&self, word: &str) -> Option<Atom> {
        if let Some(base) = &self.base {
            if let Some(atom) = base.lookup(word) {
                return Some(atom);
            }
        }

        self.words.get(word).cloned()
    }

    /// Add `word` to this pool if it is not already in it. Return the atom
    /// for the word.
    /// Total number of atoms across this pool and all ancestors.
    pub fn len(&self) -> usize {
        self.base_sym as usize + self.atoms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate all `(Atom, &str)` pairs across this pool and all ancestors.
    pub fn iter_atoms(&self) -> impl Iterator<Item = (Atom, &str)> + '_ {
        (0..self.len()).map(move |i| {
            let atom = Atom(i as AtomInt);
            let s = self.get(atom).unwrap();
            (atom, s)
        })
    }

    pub fn intern(&mut self, word: &str) -> Atom {
        if let Some(atom) = self.lookup(word) {
            return atom;
        }

        let atom =
            AtomInt::try_from(self.base_sym as usize + self.atoms.len()).expect("too many atoms");
        let atom = Atom(atom);
        if atom == TOMBSTONE_KEY {
            panic!("too many atoms");
        }
        self.words.insert(Box::from(word), atom);
        self.atoms.push(Box::from(word));
        atom
    }
}

impl<S: AsRef<str>> FromIterator<S> for AtomPool {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        let mut pool = AtomPool::new();
        for s in iter {
            pool.intern(s.as_ref());
        }
        pool
    }
}

/// A marker for an empty entry in an `Object`
const TOMBSTONE_KEY: Atom = Atom(AtomInt::MAX);

/// A value that can be used as a null value in an `Object`. The null value
/// is used when removing an entry as `Object.remove` does not actually
/// remove the entry but replaces it with a tombstone marker.
pub trait NullValue {
    fn null() -> Self;
}

impl<T: Default> NullValue for T {
    fn null() -> Self {
        T::default()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Entry<V> {
    key: Atom,
    value: V,
}

impl<V: GasSizeOf> GasSizeOf for Entry<V> {
    fn gas_size_of(&self) -> Gas {
        Gas::new(std::mem::size_of::<Atom>() as u64) + self.value.gas_size_of()
    }
}

/// A map-like data structure that uses an `AtomPool` for its keys. The data
/// structure assumes that reads are much more common than writes, and that
/// entries are rarely removed. It also assumes that each instance has
/// relatively few entries.
#[derive(Clone)]
pub struct Object<V> {
    pool: Arc<AtomPool>,
    // This could be further improved by using two `Vec`s, one for keys and
    // one for values. That would avoid losing memory to padding.
    entries: Vec<Entry<V>>,
    /// Set by `sort_by_key()`, cleared by mutations. When true, entries
    /// are in key-sorted order and callers can skip re-sorting.
    sorted: bool,
}

impl<V> Object<V> {
    /// Create a new `Object` whose keys are interned in `pool`.
    pub fn new(pool: Arc<AtomPool>) -> Self {
        Self {
            pool,
            entries: Vec::new(),
            sorted: false,
        }
    }

    /// Return the number of entries in the object. Because of tombstones,
    /// this operation has to traverse all entries
    pub fn len(&self) -> usize {
        // Because of tombstones we can't just return `self.entries.len()`.
        self.entries
            .iter()
            .filter(|entry| entry.key != TOMBSTONE_KEY)
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Find the value for `key` in the object. Return `None` if the key is
    /// not present.
    pub fn get(&self, key: &str) -> Option<&V> {
        match self.pool.lookup(key) {
            None => None,
            Some(key) => self
                .entries
                .iter()
                .find(|entry| entry.key == key)
                .map(|entry| &entry.value),
        }
    }

    /// Find the value for `atom` in the object. Return `None` if the atom
    /// is not present.
    fn get_by_atom(&self, atom: &Atom) -> Option<&V> {
        if *atom == TOMBSTONE_KEY {
            return None;
        }

        self.entries
            .iter()
            .find(|entry| &entry.key == atom)
            .map(|entry| &entry.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        ObjectIter::new(self)
    }

    /// Add or update an entry to the object. Return the value that was
    /// previously associated with the `key`. The `key` must already be part
    /// of the `AtomPool` that this object uses. Trying to set a key that is
    /// not in the pool will result in an error.
    pub fn insert<K: AsRef<str>>(&mut self, key: K, value: V) -> Result<Option<V>, Error> {
        let key = self
            .pool
            .lookup(key.as_ref())
            .ok_or_else(|| Error::NotInterned(key.as_ref().to_string()))?;
        Ok(self.insert_atom(key, value))
    }

    fn insert_atom(&mut self, key: Atom, value: V) -> Option<V> {
        if key == TOMBSTONE_KEY {
            // Ignore attempts to insert the tombstone key.
            return None;
        }

        match self.entries.iter_mut().find(|entry| entry.key == key) {
            Some(entry) => Some(std::mem::replace(&mut entry.value, value)),
            None => {
                self.sorted = false;
                self.entries.push(Entry { key, value });
                None
            }
        }
    }

    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.entries
            .iter()
            .any(|entry| self.pool.get(entry.key) == Some(key))
    }

    pub fn merge(&mut self, other: Object<V>) {
        self.sorted = false;
        if self.same_pool(&other) {
            for Entry { key, value } in other.entries {
                self.insert_atom(key, value);
            }
        } else {
            for (key, value) in other {
                self.insert(key, value).expect("pools use the same keys");
            }
        }
    }

    pub fn retain(&mut self, mut f: impl FnMut(&str, &V) -> bool) {
        self.entries.retain(|entry| {
            if entry.key == TOMBSTONE_KEY {
                // Since we are going through the trouble of removing
                // entries, remove deleted entries opportunistically.
                false
            } else {
                let key = self.pool.get(entry.key).unwrap();
                f(key, &entry.value)
            }
        })
    }

    fn same_pool(&self, other: &Object<V>) -> bool {
        Arc::ptr_eq(&self.pool, &other.pool)
    }

    pub fn atoms(&self) -> AtomIter<'_, V> {
        AtomIter::new(self)
    }

    /// Iterate entries as `(Atom, &V)` pairs, filtering tombstones.
    pub fn atom_entries(&self) -> impl Iterator<Item = (Atom, &V)> + '_ {
        self.entries
            .iter()
            .filter(|entry| entry.key != TOMBSTONE_KEY)
            .map(|entry| (entry.key, &entry.value))
    }

    /// Access the underlying atom pool.
    pub fn pool(&self) -> &Arc<AtomPool> {
        &self.pool
    }

    /// Sort entries by their string key. Tombstone entries are moved to the
    /// end. This makes subsequent iteration in key order O(n) instead of
    /// requiring an O(n log n) sort.
    pub fn sort_by_key(&mut self) {
        let pool = &self.pool;
        self.entries.sort_by(|a, b| {
            let a_key = if a.key == TOMBSTONE_KEY {
                None
            } else {
                pool.get(a.key)
            };
            let b_key = if b.key == TOMBSTONE_KEY {
                None
            } else {
                pool.get(b.key)
            };
            // None (tombstones) sort to the end
            match (a_key, b_key) {
                (Some(a), Some(b)) => a.cmp(b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        self.sorted = true;
    }

    /// Returns true if entries are known to be in key-sorted order,
    /// i.e. `sort_by_key()` was called and no unsorted insertion has
    /// happened since.
    pub fn is_sorted(&self) -> bool {
        self.sorted
    }
}

impl<V: PartialEq> Object<V> {
    fn len_ignore_atom(&self, atom: &Atom) -> usize {
        // Because of tombstones and the ignored atom, we can't just return `self.entries.len()`.
        self.entries
            .iter()
            .filter(|entry| entry.key != TOMBSTONE_KEY && entry.key != *atom)
            .count()
    }

    /// Check for equality while ignoring one particular element
    pub fn eq_ignore_key(&self, other: &Self, ignore_key: &str) -> bool {
        let ignore = self.pool.lookup(ignore_key);
        let len1 = if let Some(to_ignore) = ignore {
            self.len_ignore_atom(&to_ignore)
        } else {
            self.len()
        };
        let len2 = if let Some(to_ignore) = other.pool.lookup(ignore_key) {
            other.len_ignore_atom(&to_ignore)
        } else {
            other.len()
        };
        if len1 != len2 {
            return false;
        }

        if self.same_pool(other) {
            self.entries
                .iter()
                .filter(|e| e.key != TOMBSTONE_KEY && (ignore != Some(e.key)))
                .all(|Entry { key, value }| other.get_by_atom(key) == Some(value))
        } else {
            self.iter()
                .filter(|(key, _)| *key != ignore_key)
                .all(|(key, value)| other.get(key) == Some(value))
        }
    }
}

impl<V: NullValue> Object<V> {
    /// Remove `key` from the object and return the value that was
    /// associated with the `key`. The entry is actually not removed for
    /// efficiency reasons. It is instead replaced with an entry with a
    /// dummy key and a null value.
    pub fn remove(&mut self, key: &str) -> Option<V> {
        match self.pool.lookup(key) {
            None => None,
            Some(key) => self
                .entries
                .iter_mut()
                .find(|entry| entry.key == key)
                .map(|entry| {
                    entry.key = TOMBSTONE_KEY;
                    std::mem::replace(&mut entry.value, V::null())
                }),
        }
    }
}

pub struct ObjectIter<'a, V> {
    pool: &'a AtomPool,
    iter: std::slice::Iter<'a, Entry<V>>,
}

impl<'a, V> ObjectIter<'a, V> {
    fn new(object: &'a Object<V>) -> Self {
        Self {
            pool: object.pool.as_ref(),
            iter: object.entries.as_slice().iter(),
        }
    }
}

impl<'a, V> Iterator for ObjectIter<'a, V> {
    type Item = (&'a str, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.iter.by_ref() {
            if entry.key != TOMBSTONE_KEY {
                // unwrap: we only add entries that are backed by the pool
                let key = self.pool.get(entry.key).unwrap();
                return Some((key, &entry.value));
            }
        }
        None
    }
}

impl<'a, V> IntoIterator for &'a Object<V> {
    type Item = <ObjectIter<'a, V> as Iterator>::Item;

    type IntoIter = ObjectIter<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        ObjectIter::new(self)
    }
}

pub struct ObjectOwningIter<V> {
    pool: Arc<AtomPool>,
    iter: std::vec::IntoIter<Entry<V>>,
}

impl<V> ObjectOwningIter<V> {
    fn new(object: Object<V>) -> Self {
        Self {
            pool: object.pool.cheap_clone(),
            iter: object.entries.into_iter(),
        }
    }
}

impl<V> Iterator for ObjectOwningIter<V> {
    type Item = (Word, V);

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.iter.by_ref() {
            if entry.key != TOMBSTONE_KEY {
                // unwrap: we only add entries that are backed by the pool
                let key = self.pool.get(entry.key).unwrap();
                return Some((Word::from(key), entry.value));
            }
        }
        None
    }
}

pub struct AtomIter<'a, V> {
    iter: std::slice::Iter<'a, Entry<V>>,
}

impl<'a, V> AtomIter<'a, V> {
    fn new(object: &'a Object<V>) -> Self {
        Self {
            iter: object.entries.as_slice().iter(),
        }
    }
}

impl<'a, V> Iterator for AtomIter<'a, V> {
    type Item = Atom;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.iter.by_ref() {
            if entry.key != TOMBSTONE_KEY {
                return Some(entry.key);
            }
        }
        None
    }
}

impl<V> IntoIterator for Object<V> {
    type Item = <ObjectOwningIter<V> as Iterator>::Item;

    type IntoIter = ObjectOwningIter<V>;

    fn into_iter(self) -> Self::IntoIter {
        ObjectOwningIter::new(self)
    }
}

impl<V: CacheWeight> CacheWeight for Entry<V> {
    fn indirect_weight(&self) -> usize {
        self.value.indirect_weight()
    }
}

impl<V: CacheWeight> CacheWeight for Object<V> {
    fn indirect_weight(&self) -> usize {
        self.entries.indirect_weight()
    }
}

impl<V: std::fmt::Debug> std::fmt::Debug for Object<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.entries.fmt(f)
    }
}

impl<V: PartialEq> PartialEq for Object<V> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        if self.same_pool(other) {
            self.entries
                .iter()
                .filter(|e| e.key != TOMBSTONE_KEY)
                .all(|Entry { key, value }| other.get_by_atom(key) == Some(value))
        } else {
            self.iter()
                .all(|(key, value)| other.get(key) == Some(value))
        }
    }
}

impl<V: Eq> Eq for Object<V> {
    fn assert_receiver_is_total_eq(&self) {}
}

impl<V: Serialize> Serialize for Object<V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.iter())
    }
}

impl<V: GasSizeOf> GasSizeOf for Object<V> {
    fn gas_size_of(&self) -> Gas {
        Gas::new(std::mem::size_of::<Arc<AtomPool>>() as u64) + self.entries.gas_size_of()
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::r;

    use super::*;

    #[test]
    fn simple() {
        let mut intern = AtomPool::new();
        let hello = intern.intern("Hello");
        assert_eq!(Some(hello), intern.lookup("Hello"));
        assert_eq!(None, intern.lookup("World"));
        assert_eq!(Some("Hello"), intern.get(hello));

        // Print some size information, just for understanding better how
        // big these data structures are
        use std::mem;

        println!(
            "pool: {}, arc: {}",
            mem::size_of::<AtomPool>(),
            mem::size_of::<Arc<AtomPool>>()
        );

        println!(
            "Atom: {}, FatAtom: {}",
            mem::size_of::<Atom>(),
            mem::size_of::<FatAtom>(),
        );
        println!(
            "Entry<u64>: {}, Object<u64>: {}",
            mem::size_of::<Entry<u64>>(),
            mem::size_of::<Object<u64>>()
        );
        println!(
            "Entry<Value>: {}, Object<Value>: {}, r::Value: {}",
            mem::size_of::<Entry<r::Value>>(),
            mem::size_of::<Object<r::Value>>(),
            mem::size_of::<r::Value>()
        );
    }

    #[test]
    fn stacked() {
        let mut base = AtomPool::new();
        let bsym = base.intern("base");
        let isym = base.intern("intern");
        let base = Arc::new(base);

        let mut intern = base.child();
        assert_eq!(Some(bsym), intern.lookup("base"));

        assert_eq!(bsym, intern.intern("base"));
        let hello = intern.intern("hello");
        assert_eq!(None, base.get(hello));
        assert_eq!(Some("hello"), intern.get(hello));
        assert_eq!(None, base.lookup("hello"));
        assert_eq!(Some(hello), intern.lookup("hello"));
        assert_eq!(Some(isym), base.lookup("intern"));
        assert_eq!(Some(isym), intern.lookup("intern"));
    }

    fn make_pool(words: Vec<&str>) -> Arc<AtomPool> {
        let mut pool = AtomPool::new();
        for word in words {
            pool.intern(word);
        }
        Arc::new(pool)
    }

    fn make_obj(pool: Arc<AtomPool>, entries: Vec<(&str, usize)>) -> Object<usize> {
        let mut obj: Object<usize> = Object::new(pool);
        for (k, v) in entries {
            obj.insert(k, v).unwrap();
        }
        obj
    }

    #[test]
    fn object_eq() {
        // Make an object `{ "one": 1, "two": 2 }` that has a removed key
        // `three` in it to make sure equality checking ignores removed keys
        fn make_obj1(pool: Arc<AtomPool>) -> Object<usize> {
            let mut obj = make_obj(pool, vec![("one", 1), ("two", 2), ("three", 3)]);
            obj.remove("three");
            obj
        }

        // Make two pools with the same atoms, but different order
        let pool1 = make_pool(vec!["one", "two", "three"]);
        let pool2 = make_pool(vec!["three", "two", "one"]);

        // Make two objects with the same keys and values in the same order
        // but different pools
        let obj1 = make_obj1(pool1.clone());
        let obj2 = make_obj(pool2.clone(), vec![("one", 1), ("two", 2)]);
        assert_eq!(obj1, obj2);

        // Make two objects with the same keys and values in different order
        // and with  different pools
        let obj1 = make_obj1(pool1.clone());
        let obj2 = make_obj(pool2.clone(), vec![("two", 2), ("one", 1)]);
        assert_eq!(obj1, obj2);

        // Check that two objects using the same pools and the same keys and
        // values but in different order are equal
        let pool = pool1;
        let obj1 = make_obj1(pool.clone());
        let obj2 = make_obj(pool.clone(), vec![("two", 2), ("one", 1)]);
        assert_eq!(obj1, obj2);
    }

    #[test]
    fn object_remove() {
        let pool = make_pool(vec!["one", "two", "three"]);
        let mut obj = make_obj(pool.clone(), vec![("one", 1), ("two", 2)]);

        assert_eq!(Some(1), obj.remove("one"));
        assert_eq!(None, obj.get("one"));
        assert_eq!(Some(&2), obj.get("two"));

        let entries = obj.iter().collect::<Vec<_>>();
        assert_eq!(vec![("two", &2)], entries);

        assert_eq!(None, obj.remove("one"));
        let entries = obj.into_iter().collect::<Vec<_>>();
        assert_eq!(vec![(Word::from("two"), 2)], entries);
    }

    #[test]
    fn object_insert() {
        let pool = make_pool(vec!["one", "two", "three"]);
        let mut obj = make_obj(pool.clone(), vec![("one", 1), ("two", 2)]);

        assert_eq!(Some(1), obj.insert("one", 17).unwrap());
        assert_eq!(Some(&17), obj.get("one"));
        assert_eq!(Some(&2), obj.get("two"));
        assert!(obj.insert("not interned", 42).is_err());

        let entries = obj.iter().collect::<Vec<_>>();
        assert_eq!(vec![("one", &17), ("two", &2)], entries);

        assert_eq!(None, obj.insert("three", 3).unwrap());
        let entries = obj.into_iter().collect::<Vec<_>>();
        assert_eq!(
            vec![
                (Word::from("one"), 17),
                (Word::from("two"), 2),
                (Word::from("three"), 3)
            ],
            entries
        );
    }

    #[test]
    fn object_remove_insert() {
        let pool = make_pool(vec!["one", "two", "three"]);
        let mut obj = make_obj(pool.clone(), vec![("one", 1), ("two", 2)]);

        // Remove an entry
        assert_eq!(Some(1), obj.remove("one"));
        assert_eq!(None, obj.get("one"));

        let entries = obj.iter().collect::<Vec<_>>();
        assert_eq!(vec![("two", &2)], entries);

        // And insert it again
        assert_eq!(None, obj.insert("one", 1).unwrap());

        let entries = obj.iter().collect::<Vec<_>>();
        assert_eq!(vec![("two", &2), ("one", &1)], entries);

        let entries = obj.into_iter().collect::<Vec<_>>();
        assert_eq!(
            vec![(Word::from("two"), 2), (Word::from("one"), 1)],
            entries
        );
    }

    #[test]
    fn object_merge() {
        let pool1 = make_pool(vec!["one", "two", "three"]);
        let pool2 = make_pool(vec!["three", "two", "one"]);

        // Merge objects with different pools
        let mut obj1 = make_obj(pool1.clone(), vec![("one", 1), ("two", 2)]);
        let obj2 = make_obj(pool2.clone(), vec![("one", 11), ("three", 3)]);

        obj1.merge(obj2);
        let entries = obj1.into_iter().collect::<Vec<_>>();
        assert_eq!(
            vec![
                (Word::from("one"), 11),
                (Word::from("two"), 2),
                (Word::from("three"), 3)
            ],
            entries
        );

        // Merge objects with the same pool
        let mut obj1 = make_obj(pool1.clone(), vec![("one", 1), ("two", 2)]);
        let obj2 = make_obj(pool1.clone(), vec![("one", 11), ("three", 3)]);
        obj1.merge(obj2);
        let entries = obj1.into_iter().collect::<Vec<_>>();
        assert_eq!(
            vec![
                (Word::from("one"), 11),
                (Word::from("two"), 2),
                (Word::from("three"), 3)
            ],
            entries
        );
    }
}
