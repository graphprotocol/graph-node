use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use graph::components::metrics::{counter_with_labels, gauge_with_labels};
use graph::prelude::{MetricsRegistry as MetricsRegistryTrait, *};

#[derive(Clone)]
pub struct MetricsRegistry {
    logger: Logger,
    registry: Arc<Registry>,
    register_errors: Box<Counter>,
    unregister_errors: Box<Counter>,
    registered_metrics: Box<Gauge>,

    /// Global metrics are lazily initialized and identified by
    /// the `Desc.id` that hashes the name and const label values
    global_counters: Arc<RwLock<HashMap<u64, Counter>>>,
    global_gauges: Arc<RwLock<HashMap<u64, Gauge>>>,
}

impl MetricsRegistry {
    pub fn new(logger: Logger, registry: Arc<Registry>) -> Self {
        // Generate internal metrics
        let register_errors = Self::gen_register_errors_counter(registry.clone());
        let unregister_errors = Self::gen_unregister_errors_counter(registry.clone());
        let registered_metrics = Self::gen_registered_metrics_gauge(registry.clone());

        MetricsRegistry {
            logger: logger.new(o!("component" => String::from("MetricsRegistry"))),
            registry,
            register_errors,
            unregister_errors,
            registered_metrics,
            global_counters: Arc::new(RwLock::new(HashMap::new())),
            global_gauges: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn gen_register_errors_counter(registry: Arc<Registry>) -> Box<Counter> {
        let opts = Opts::new(
            String::from("metrics_register_errors"),
            String::from("Counts Prometheus metrics register errors"),
        );
        let counter = Box::new(
            Counter::with_opts(opts).expect("failed to create `metrics_register_errors` counter"),
        );
        registry
            .register(counter.clone())
            .expect("failed to register `metrics_register_errors` counter");
        counter
    }

    fn gen_unregister_errors_counter(registry: Arc<Registry>) -> Box<Counter> {
        let opts = Opts::new(
            String::from("metrics_unregister_errors"),
            String::from("Counts Prometheus metrics unregister errors"),
        );
        let counter = Box::new(
            Counter::with_opts(opts).expect("failed to create `metrics_unregister_errors` counter"),
        );
        registry
            .register(counter.clone())
            .expect("failed to register `metrics_unregister_errors` counter");
        counter
    }

    fn gen_registered_metrics_gauge(registry: Arc<Registry>) -> Box<Gauge> {
        let opts = Opts::new(
            String::from("registered_metrics"),
            String::from("Tracks the number of registered metrics on the node"),
        );
        let gauge =
            Box::new(Gauge::with_opts(opts).expect("failed to create `registered_metrics` gauge"));
        registry
            .register(gauge.clone())
            .expect("failed to register `registered_metrics` gauge");
        gauge
    }
}

impl MetricsRegistryTrait for MetricsRegistry {
    fn register(&self, name: &str, c: Box<dyn Collector>) {
        let err = match self.registry.register(c).err() {
            None => {
                self.registered_metrics.inc();
                return;
            }
            Some(err) => {
                self.register_errors.inc();
                err
            }
        };
        match err {
            PrometheusError::AlreadyReg => {
                error!(
                    self.logger,
                    "registering metric [{}] because it was already registered", name,
                );
            }
            PrometheusError::InconsistentCardinality { expect, got } => {
                error!(
                    self.logger,
                    "registering metric [{}] failed due to inconsistent caridinality, expected = {} got = {}",
                    name,
                    expect,
                    got,
                );
            }
            PrometheusError::Msg(msg) => {
                error!(
                    self.logger,
                    "registering metric [{}] failed because: {}", name, msg,
                );
            }
            PrometheusError::Io(err) => {
                error!(
                    self.logger,
                    "registering metric [{}] failed due to io error: {}", name, err,
                );
            }
            PrometheusError::Protobuf(err) => {
                error!(
                    self.logger,
                    "registering metric [{}] failed due to protobuf error: {}", name, err
                );
            }
        };
    }

    fn global_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Counter, PrometheusError> {
        let counter = counter_with_labels(name, help, const_labels)?;
        let id = counter.desc().first().unwrap().id;
        let maybe_counter = self.global_counters.read().unwrap().get(&id).cloned();
        if let Some(counter) = maybe_counter {
            Ok(counter)
        } else {
            self.register(name, Box::new(counter.clone()));
            self.global_counters
                .write()
                .unwrap()
                .insert(id, counter.clone());
            Ok(counter)
        }
    }

    fn global_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Gauge, PrometheusError> {
        let gauge = gauge_with_labels(name, help, const_labels)?;
        let id = gauge.desc().first().unwrap().id;
        let maybe_gauge = self.global_gauges.read().unwrap().get(&id).cloned();
        if let Some(gauge) = maybe_gauge {
            Ok(gauge.clone())
        } else {
            self.register(name, Box::new(gauge.clone()));
            self.global_gauges
                .write()
                .unwrap()
                .insert(id, gauge.clone());
            Ok(gauge)
        }
    }

    fn unregister(&self, metric: Box<dyn Collector>) {
        match self.registry.unregister(metric) {
            Ok(_) => {
                self.registered_metrics.dec();
            }
            Err(e) => {
                self.unregister_errors.inc();
                error!(self.logger, "Unregistering metric failed = {:?}", e,);
            }
        };
    }
}

#[test]
fn global_counters_are_shared() {
    use graph::log;

    let logger = log::logger(false);
    let prom_reg = Arc::new(Registry::new());
    let registry = MetricsRegistry::new(logger, prom_reg.clone());

    fn check_counters(
        registry: &MetricsRegistry,
        name: &str,
        const_labels: HashMap<String, String>,
    ) {
        let c1 = registry
            .global_counter(name, "help me", const_labels.clone())
            .expect("first test counter");
        let c2 = registry
            .global_counter(name, "help me", const_labels)
            .expect("second test counter");
        let desc1 = c1.desc();
        let desc2 = c2.desc();
        let d1 = desc1.first().unwrap();
        let d2 = desc2.first().unwrap();

        // Registering the same metric with the same name and
        // const labels twice works and returns the same metric (logically)
        assert_eq!(d1.id, d2.id, "counters: {}", name);

        // They share the reported values
        c1.inc_by(7.0);
        c2.inc_by(2.0);
        assert_eq!(9.0, c1.get(), "counters: {}", name);
        assert_eq!(9.0, c2.get(), "counters: {}", name);
    }

    check_counters(&registry, "nolabels", HashMap::new());

    let const_labels = {
        let mut map = HashMap::new();
        map.insert("pool".to_owned(), "main".to_owned());
        map
    };
    check_counters(&registry, "pool", const_labels);

    let const_labels = {
        let mut map = HashMap::new();
        map.insert("pool".to_owned(), "replica0".to_owned());
        map
    };
    check_counters(&registry, "pool", const_labels);
}
