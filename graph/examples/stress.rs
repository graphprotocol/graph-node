use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use graph::prelude::q;
use rand::{thread_rng, Rng};
use structopt::StructOpt;

use graph::util::cache_weight::CacheWeight;
use graph::util::lfu_cache::LfuCache;

// Use a custom allocator that tracks how much memory the program
// has allocated overall

struct Counter;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), SeqCst);
        }
        return ret;
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
    fn sample(&self, size: usize) -> Self::Item;
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
    fn sample(&self, size: usize) -> Self::Item {
        self[0..size].into()
    }
}

/// Template for testing caching of `Vec<usize>`
impl Template<Vec<usize>> for Vec<usize> {
    type Item = Vec<usize>;

    fn create(size: usize) -> Self {
        Vec::from_iter(0..size)
    }
    fn sample(&self, size: usize) -> Self::Item {
        self[0..size].into()
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

    fn sample(&self, size: usize) -> Self::Item {
        HashMap::from_iter(
            self.iter()
                .take(size)
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        )
    }
}

type ValueMap = BTreeMap<String, q::Value>;

/// Template for testing roughly a GraphQL response, i.e., a `BTreeMap<String, Value>`
impl Template<ValueMap> for ValueMap {
    type Item = ValueMap;

    fn create(size: usize) -> Self {
        let mut map = BTreeMap::new();
        for i in 0..size {
            let value = match i % 9 {
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
                _ => q::Value::String(format!("other{}", i)),
            };
            map.insert(format!("val{}", i), value);
        }
        map
    }

    fn sample(&self, size: usize) -> Self::Item {
        BTreeMap::from_iter(
            self.iter()
                .take(size)
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        )
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

    fn sample(&self, size: usize) -> T::Item {
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
    #[structopt(short, long, default_value = "1000")]
    niter: usize,
    #[structopt(short, long, default_value = "10")]
    print_count: usize,
    #[structopt(short, long, default_value = "1024")]
    obj_size: usize,
    #[structopt(short, long, default_value = "1000000")]
    cache_size: usize,
    #[structopt(short, long, default_value = "vec")]
    template: String,
    #[structopt(short, long)]
    samples: bool,
    #[structopt(short, long)]
    fixed: bool,
}

fn stress<T: Template<T, Item = T>>(opt: &Opt) {
    let mut cacheable: Cacheable<T> = Cacheable::new(opt.obj_size);

    println!("type: {}", cacheable.name());
    println!(
        "obj: {} iterations: {} cache_size: {}",
        cacheable.template.weight(),
        opt.niter,
        opt.cache_size
    );
    println!("heap_factor is heap_size / cache_size");

    let mut rng = thread_rng();
    let base_mem = ALLOCATED.load(SeqCst);
    let print_mod = opt.niter / opt.print_count + 1;
    let mut should_print = true;
    for key in 0..opt.niter {
        should_print = should_print || key % print_mod == 0;
        let before_mem = ALLOCATED.load(SeqCst);
        if let Some((evicted, _, new_weight)) = cacheable.cache.evict(opt.cache_size) {
            let after_mem = ALLOCATED.load(SeqCst);
            if should_print {
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
        cacheable.cache.insert(key, cacheable.sample(size));
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
    } else {
        println!("unknown value for --template")
    }
}
