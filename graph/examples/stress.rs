use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use graph::prelude::{lazy_static, q};
use rand::{thread_rng, Rng};
use structopt::StructOpt;

use graph::util::cache_weight::CacheWeight;
use graph::util::lfu_cache::LfuCache;

// Use a custom allocator that tracks how much memory the program
// has allocated overall

struct Counter;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    // Set 'MAP_MEASURE' to something to use the `CacheWeight` defined here
    // in the `btree` module for `BTreeMap`. If this is not set, use the
    // estimate from `graph::util::cache_weight`
    static ref MAP_MEASURE: bool = std::env::var("MAP_MEASURE").ok().is_some();

    // When running the `valuemap` test for BTreeMap, put maps into the
    // values of the generated maps
    static ref NESTED_MAP: bool =  std::env::var("NESTED_MAP").ok().is_some();
}
// Yes, a global variable. It gets set at the beginning of `main`
static mut PRINT_SAMPLES: bool = false;

/// Helpers to estimate the size of a `BTreeMap`. Everything in this module,
/// except for `node_size()` is copied from `std::collections::btree`.
///
/// It is not possible to know how many nodes a BTree has, as
/// `BTreeMap` does not expose its depth or any other detail about
/// the true size of the BTree. We estimate that size, assuming the
/// average case, i.e., a BTree where every node has the average
/// between the minimum and maximum number of entries per node, i.e.,
/// the average of (B-1) and (2*B-1) entries, which we call
/// `NODE_FILL`. The number of leaf nodes in the tree is then the
/// number of entries divided by `NODE_FILL`, and the number of
/// interior nodes can be determined by dividing the number of nodes
/// at the child level by `NODE_FILL`

/// The other difficulty is that the structs with which `BTreeMap`
/// represents internal and leaf nodes are not public, so we can't
/// get their size with `std::mem::size_of`; instead, we base our
/// estimates of their size on the current `std` code, assuming that
/// these structs will not change

mod btree {
    use std::mem;
    use std::{mem::MaybeUninit, ptr::NonNull};

    const B: usize = 6;
    const CAPACITY: usize = 2 * B - 1;

    /// Assume BTree nodes are this full (average of minimum and maximum fill)
    const NODE_FILL: usize = ((B - 1) + (2 * B - 1)) / 2;

    type BoxedNode<K, V> = NonNull<LeafNode<K, V>>;

    struct InternalNode<K, V> {
        _data: LeafNode<K, V>,

        /// The pointers to the children of this node. `len + 1` of these are considered
        /// initialized and valid, except that near the end, while the tree is held
        /// through borrow type `Dying`, some of these pointers are dangling.
        _edges: [MaybeUninit<BoxedNode<K, V>>; 2 * B],
    }

    struct LeafNode<K, V> {
        /// We want to be covariant in `K` and `V`.
        _parent: Option<NonNull<InternalNode<K, V>>>,

        /// This node's index into the parent node's `edges` array.
        /// `*node.parent.edges[node.parent_idx]` should be the same thing as `node`.
        /// This is only guaranteed to be initialized when `parent` is non-null.
        _parent_idx: MaybeUninit<u16>,

        /// The number of keys and values this node stores.
        _len: u16,

        /// The arrays storing the actual data of the node. Only the first `len` elements of each
        /// array are initialized and valid.
        _keys: [MaybeUninit<K>; CAPACITY],
        _vals: [MaybeUninit<V>; CAPACITY],
    }

    pub fn node_size<K, V>(map: &std::collections::BTreeMap<K, V>) -> usize {
        // Measure the size of internal and leaf nodes directly - that's why
        // we copied all this code from `std`
        let ln_sz = mem::size_of::<LeafNode<K, V>>();
        let in_sz = mem::size_of::<InternalNode<K, V>>();

        // Estimate the number of internal and leaf nodes based on the only
        // thing we can measure about a BTreeMap, the number of entries in
        // it, and use our `NODE_FILL` assumption to estimate how the tree
        // is structured. We try to be very good for small maps, since
        // that's what we use most often in our code. This estimate is only
        // for the indirect weight of the `BTreeMap`
        let (leaves, int_nodes) = if map.is_empty() {
            // An empty tree has no indirect weight
            (0, 0)
        } else if map.len() <= CAPACITY {
            // We only have the root node
            (1, 0)
        } else {
            // Estimate based on our `NODE_FILL` assumption
            let leaves = map.len() / NODE_FILL + 1;
            let mut prev_level = leaves / NODE_FILL + 1;
            let mut int_nodes = prev_level;
            while prev_level > 1 {
                int_nodes += prev_level;
                prev_level = prev_level / NODE_FILL + 1;
            }
            (leaves, int_nodes)
        };

        let sz = leaves * ln_sz + int_nodes * in_sz;

        if unsafe { super::PRINT_SAMPLES } {
            println!(
                " btree: leaves={} internal={} sz={} ln_sz={} in_sz={} len={}",
                leaves,
                int_nodes,
                sz,
                ln_sz,
                in_sz,
                map.len()
            );
        }
        sz
    }
}

struct MapMeasure<K, V>(BTreeMap<K, V>);

impl<K, V> Default for MapMeasure<K, V> {
    fn default() -> MapMeasure<K, V> {
        MapMeasure(BTreeMap::new())
    }
}

impl<K: CacheWeight, V: CacheWeight> CacheWeight for MapMeasure<K, V> {
    fn indirect_weight(&self) -> usize {
        if *MAP_MEASURE {
            let kv_sz = self
                .0
                .iter()
                .map(|(key, value)| key.indirect_weight() + value.indirect_weight())
                .sum::<usize>();
            let node_sz = btree::node_size(&self.0);
            kv_sz + node_sz
        } else {
            self.0.indirect_weight()
        }
    }
}

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), SeqCst);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED.fetch_sub(layout.size(), SeqCst);
    }
}

#[global_allocator]
static A: Counter = Counter;

// Setup to make checking different data types and how they interact
// with cache size easier

/// The template of an object we want to cache
trait Template<T>: CacheWeight + Default {
    type Item;

    // Create a new test object
    fn create(size: usize) -> Self;

    // Return a sample of this test object of the given `size`. There's no
    // fixed definition of 'size', other than that smaller sizes will
    // take less memory than larger ones
    fn sample(&self, size: usize) -> Box<Self::Item>;
}

/// Template for testing caching of `String`
impl Template<String> for String {
    type Item = String;

    fn create(size: usize) -> Self {
        let mut s = String::with_capacity(size);
        for _ in 0..size {
            s.push('x');
        }
        s
    }
    fn sample(&self, size: usize) -> Box<Self::Item> {
        Box::new(self[0..size].into())
    }
}

/// Template for testing caching of `Vec<usize>`
impl Template<Vec<usize>> for Vec<usize> {
    type Item = Vec<usize>;

    fn create(size: usize) -> Self {
        Vec::from_iter(0..size)
    }
    fn sample(&self, size: usize) -> Box<Self::Item> {
        Box::new(self[0..size].into())
    }
}

/// Template for testing caching of `HashMap<String, String>`
impl Template<HashMap<String, String>> for HashMap<String, String> {
    type Item = Self;

    fn create(size: usize) -> Self {
        let mut map = HashMap::new();
        for i in 0..size {
            map.insert(format!("key{}", i), format!("value{}", i));
        }
        map
    }

    fn sample(&self, size: usize) -> Box<Self::Item> {
        Box::new(HashMap::from_iter(
            self.iter()
                .take(size)
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        ))
    }
}

type ValueMap = MapMeasure<String, q::Value>;

/// Template for testing roughly a GraphQL response, i.e., a `BTreeMap<String, Value>`
impl Template<ValueMap> for ValueMap {
    type Item = ValueMap;

    fn create(size: usize) -> Self {
        let mut map = BTreeMap::new();
        let modulus = if *NESTED_MAP { 9 } else { 8 };

        for i in 0..size {
            let value = match i % modulus {
                0 => q::Value::Boolean(i % 11 > 5),
                1 => q::Value::Int((i as i32).into()),
                2 => q::Value::Null,
                3 => q::Value::Float(i as f64 / 17.0),
                4 => q::Value::Enum(format!("enum{}", i)),
                5 => q::Value::String(format!("string{}", i)),
                6 => q::Value::Variable(format!("var{}", i)),
                7 => {
                    let vals = (0..(i % 51)).map(|i| q::Value::String(format!("list{}", i)));
                    q::Value::List(Vec::from_iter(vals))
                }
                8 => {
                    let mut map = BTreeMap::new();
                    for j in 0..(i % 51) {
                        map.insert(format!("key{}", j), q::Value::String(format!("value{}", j)));
                    }
                    q::Value::Object(map)
                }
                _ => unreachable!(),
            };
            map.insert(format!("val{}", i), value);
        }
        MapMeasure(map)
    }

    fn sample(&self, size: usize) -> Box<Self::Item> {
        Box::new(MapMeasure(BTreeMap::from_iter(
            self.0
                .iter()
                .take(size)
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        )))
    }
}

type UsizeMap = MapMeasure<usize, usize>;

/// Template for testing roughly a GraphQL response, i.e., a `BTreeMap<String, Value>`
impl Template<UsizeMap> for UsizeMap {
    type Item = UsizeMap;

    fn create(size: usize) -> Self {
        let mut map = BTreeMap::new();
        for i in 0..size {
            map.insert(i * 2, i * 3);
        }
        MapMeasure(map)
    }

    fn sample(&self, size: usize) -> Box<Self::Item> {
        Box::new(MapMeasure(BTreeMap::from_iter(
            self.0
                .iter()
                .take(size)
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        )))
    }
}

/// Helper to deal with different template objects
struct Cacheable<T> {
    cache: LfuCache<usize, T>,
    template: T,
}

impl<T: Template<T>> Cacheable<T> {
    fn new(size: usize) -> Self {
        Cacheable {
            cache: LfuCache::new(),
            template: T::create(size),
        }
    }

    fn sample(&self, size: usize) -> Box<T::Item> {
        self.template.sample(size)
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

// Command line arguments
#[derive(StructOpt)]
#[structopt(name = "stress", about = "Stress test for the LFU Cache")]
struct Opt {
    /// Number of cache evictions and insertions
    #[structopt(short, long, default_value = "1000")]
    niter: usize,
    /// Print this many intermediate messages
    #[structopt(short, long, default_value = "10")]
    print_count: usize,
    /// Use objects of size 0 up to this size, chosen unifromly randomly
    /// unless `--fixed` is given
    #[structopt(short, long, default_value = "1024")]
    obj_size: usize,
    #[structopt(short, long, default_value = "1000000")]
    cache_size: usize,
    #[structopt(short, long, default_value = "vec")]
    template: String,
    #[structopt(short, long)]
    samples: bool,
    /// Always use objects of size `--obj-size`
    #[structopt(short, long)]
    fixed: bool,
}

fn stress<T: Template<T, Item = T>>(opt: &Opt) {
    let mut cacheable: Cacheable<T> = Cacheable::new(opt.obj_size);

    println!("type: {}", cacheable.name());
    println!(
        "obj: {} iterations: {} cache_size: {}\n",
        cacheable.template.weight(),
        opt.niter,
        opt.cache_size
    );

    let mut rng = thread_rng();
    let base_mem = ALLOCATED.load(SeqCst);
    let print_mod = opt.niter / opt.print_count + 1;
    let mut should_print = true;
    let mut print_header = true;
    for key in 0..opt.niter {
        should_print = should_print || key % print_mod == 0;
        let before_mem = ALLOCATED.load(SeqCst);
        if let Some((evicted, _, new_weight)) = cacheable.cache.evict(opt.cache_size) {
            let after_mem = ALLOCATED.load(SeqCst);
            if should_print {
                if print_header {
                    println!("heap_factor is heap_size / cache_size");
                    print_header = false;
                }

                let heap_factor = (after_mem - base_mem) as f64 / opt.cache_size as f64;
                println!(
                    "evicted: {:6}  dropped: {:6} new_weight: {:8} heap_factor: {:0.2}  ",
                    evicted,
                    before_mem - after_mem,
                    new_weight,
                    heap_factor
                );
                should_print = false;
            }
        }
        let size = if opt.fixed || opt.obj_size == 0 {
            opt.obj_size
        } else {
            rng.gen_range(0, opt.obj_size)
        };
        let before = ALLOCATED.load(SeqCst);
        let sample = cacheable.sample(size);
        if opt.samples {
            println!(
                "sample: weight {:6} alloc {:6}",
                sample.weight(),
                ALLOCATED.load(SeqCst) - before,
            );
        }
        cacheable.cache.insert(key, *cacheable.sample(size));
    }
}

/// This program constructs a template object of size `obj_size` and then
/// inserts a sample of size up to `obj_size` into the cache `niter` times.
/// The cache is limited to `cache_size` total weight, and we call `evict`
/// before each insertion into the cache.
///
/// After each `evict`, we check how much heap we have currently allocated
/// and print that roughly `print_count` times over the run of the program.
/// The most important measure is the `heap_factor`, which is the ratio of
/// memory used on the heap since we started inserting into the cache to
/// the target `cache_size`
pub fn main() {
    let opt = Opt::from_args();
    unsafe { PRINT_SAMPLES = opt.samples }

    // Use different Cacheables to see how the cache manages memory with
    // different types of cache entries. Uncomment one of the 'let mut cacheable'
    // lines
    if opt.template == "vec" {
        // With Vec<usize> we stay within between opt.cache_size and 3*opt.cache_size
        // Larger heap factors for very small arrays
        // obj_size  |  heap factor
        //   10      |     4.02
        //   20      |     2.39
        //   30      |     2.40
        //   50      |     1.76
        //  100      |     1.38
        // 1000      |     1.05
        stress::<Vec<usize>>(&opt);
    } else if opt.template == "hashmap" {
        // Cache HashMap<String, String>
        // The heap factor ranges between 2.23 (size 3) and 1.06 (size 100)
        //let mut cacheable: Cacheable<HashMap<String, String>> = Cacheable::new(opt.obj_size);
        stress::<HashMap<String, String>>(&opt);
    } else if opt.template == "valuemap" {
        // Cache BTreeMap<String, Value>
        // obj_size  |  heap factor
        //    3      |     16.51
        //    5      |     12.07
        //   10      |      4.64
        //   50      |      3.07
        //  100      |      2.94
        //
        // The above is for a weight calculation that does not take the
        // allocated, unused space in the BTree into account. With a guess
        // at those, the above heap factors range from 1.14 to 0.88, with the
        // exception of obj_size 0 where we get a factor of 2.88, but that
        // must be caused by some other effect
        stress::<ValueMap>(&opt);
    } else if opt.template == "string" {
        stress::<String>(&opt);
    } else if opt.template == "usizemap" {
        stress::<UsizeMap>(&opt)
    } else {
        println!("unknown value for --template")
    }
}
