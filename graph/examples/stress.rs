use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use graphql_parser::query as q;
use rand::{thread_rng, Rng};
use structopt::StructOpt;

use graph::util::cache_weight::CacheWeight;
use graph::util::lfu_cache::LfuCache;

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
}

trait Template<T> {
    type Item;

    fn create(size: usize) -> Self;
    fn sample(&self, size: usize) -> Self::Item;
}

impl Template<Vec<usize>> for Vec<usize> {
    type Item = Vec<usize>;

    fn create(size: usize) -> Self {
        Vec::from_iter(0..size)
    }
    fn sample(&self, size: usize) -> Self::Item {
        self[0..size].into()
    }
}

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

struct Cacheable<T> {
    cache: LfuCache<usize, T>,
    template: T,
}

impl<T: CacheWeight + Default + Template<T>> Cacheable<T> {
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

pub fn main() {
    let opt = Opt::from_args();

    // Use different Cacheables to see how the cache manages memory with
    // different types of cache entries. Uncomment one of the 'let mut cacheable'
    // lines

    // With Vec<usize> we stay within between opt.cache_size and 3*opt.cache_size
    // Larger heap factors for very small arrays
    // obj_size  |  heap factor
    //   10      |     4.02
    //   20      |     2.39
    //   30      |     2.40
    //   50      |     1.76
    //  100      |     1.38
    // 1000      |     1.05
    // let mut cacheable: Cacheable<Vec<usize>> = Cacheable::new(opt.obj_size);

    // Cache HashMap<String, String>
    // The heap factor ranges between 2.23 (size 3) and 1.06 (size 100)
    //let mut cacheable: Cacheable<HashMap<String, String>> = Cacheable::new(opt.obj_size);

    // Cache BTreeMap<String, Value>
    // obj_size  |  heap factor
    //    3      |     16.51
    //    5      |     12.07
    //   10      |      4.64
    //   50      |      3.07
    //  100      |      2.94
    let mut cacheable: Cacheable<ValueMap> = Cacheable::new(opt.obj_size);

    println!("type: {}", cacheable.name());
    println!(
        "obj: {} nitems: {} cache_size: {}",
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
        if let Some((evicted, _, new_weight)) = cacheable.cache.evict(opt.cache_size) {
            if should_print {
                let heap_factor =
                    (ALLOCATED.load(SeqCst) - base_mem) as f64 / opt.cache_size as f64;
                println!(
                    "evicted: {:6}  new_weight: {:8} heap_factor: {:0.2}  ",
                    evicted, new_weight, heap_factor
                );
                should_print = false;
            }
        }
        let size = rng.gen_range(2, opt.obj_size);
        cacheable.cache.insert(key, cacheable.sample(size));
    }
}
