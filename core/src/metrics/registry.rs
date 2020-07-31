use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use graph::prelude::{MetricsRegistry as MetricsRegistryTrait, *};

pub struct MetricsRegistry {
    logger: Logger,
    registry: Arc<Registry>,
    const_labels: HashMap<String, String>,
    register_errors: Box<Counter>,
    unregister_errors: Box<Counter>,
    registered_metrics: Box<Gauge>,

    /// Global metrics are are lazily initialized and identified by name.
    global_counters: Arc<RwLock<HashMap<String, Counter>>>,
    global_gauges: Arc<RwLock<HashMap<String, Gauge>>>,
}

impl MetricsRegistry {
    pub fn new(logger: Logger, registry: Arc<Registry>) -> Self {
        let const_labels = HashMap::new();

        // Generate internal metrics
        let register_errors = Self::gen_register_errors_counter(registry.clone());
        let unregister_errors = Self::gen_unregister_errors_counter(registry.clone());
        let registered_metrics = Self::gen_registered_metrics_gauge(registry.clone());

        MetricsRegistry {
            logger: logger.new(o!("component" => String::from("MetricsRegistry"))),
            registry,
            const_labels,
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

    pub fn register(&self, name: &str, c: Box<dyn Collector>) {
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
            PrometheusError::InconsistentCardinality(expected, got) => {
                error!(
                    self.logger,
                    "registering metric [{}] failed due to inconsistent caridinality, expected = {} got = {}",
                    name,
                    expected,
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
}

impl Clone for MetricsRegistry {
    fn clone(&self) -> Self {
        return Self {
            logger: self.logger.clone(),
            registry: self.registry.clone(),
            const_labels: self.const_labels.clone(),
            register_errors: self.register_errors.clone(),
            unregister_errors: self.unregister_errors.clone(),
            registered_metrics: self.registered_metrics.clone(),
            global_counters: self.global_counters.clone(),
            global_gauges: self.global_gauges.clone(),
        };
    }
}

impl MetricsRegistryTrait for MetricsRegistry {
    fn new_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Box<Gauge>, PrometheusError> {
        let labels: HashMap<String, String> = self
            .const_labels
            .clone()
            .into_iter()
            .chain(const_labels)
            .collect();
        let opts = Opts::new(name.clone(), help).const_labels(labels);
        let gauge = Box::new(Gauge::with_opts(opts)?);
        self.register(name, gauge.clone());
        Ok(gauge)
    }

    fn new_gauge_vec(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        variable_labels: Vec<String>,
    ) -> Result<Box<GaugeVec>, PrometheusError> {
        let labels: HashMap<String, String> = self
            .const_labels
            .clone()
            .into_iter()
            .chain(const_labels)
            .collect();
        let opts = Opts::new(name.clone(), help).const_labels(labels);
        let gauges = Box::new(GaugeVec::new(
            opts,
            variable_labels
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, gauges.clone());
        Ok(gauges)
    }

    fn new_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Box<Counter>, PrometheusError> {
        let labels: HashMap<String, String> = self
            .const_labels
            .clone()
            .into_iter()
            .chain(const_labels)
            .collect();
        let opts = Opts::new(name.clone(), help).const_labels(labels);
        let counter = Box::new(Counter::with_opts(opts)?);
        self.register(name, counter.clone());
        Ok(counter)
    }

    fn global_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Counter, PrometheusError> {
        let maybe_counter = self.global_counters.read().unwrap().get(name).cloned();
        if let Some(counter) = maybe_counter {
            Ok(counter.clone())
        } else {
            let counter = *self.new_counter(&name, &help, const_labels)?;
            self.global_counters
                .write()
                .unwrap()
                .insert(name.to_owned(), counter.clone());
            Ok(counter)
        }
    }

    fn global_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Gauge, PrometheusError> {
        let maybe_gauge = self.global_gauges.read().unwrap().get(name).cloned();
        if let Some(gauge) = maybe_gauge {
            Ok(gauge.clone())
        } else {
            let gauge = *self.new_gauge(name, help, const_labels)?;
            self.global_gauges
                .write()
                .unwrap()
                .insert(name.to_owned(), gauge.clone());
            Ok(gauge)
        }
    }

    fn new_counter_vec(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        variable_labels: Vec<String>,
    ) -> Result<Box<CounterVec>, PrometheusError> {
        let labels: HashMap<String, String> = self
            .const_labels
            .clone()
            .into_iter()
            .chain(const_labels)
            .collect();
        let opts = Opts::new(name.clone(), help).const_labels(labels);
        let counters = Box::new(CounterVec::new(
            opts,
            variable_labels
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, counters.clone());
        Ok(counters)
    }

    fn new_histogram(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        buckets: Vec<f64>,
    ) -> Result<Box<Histogram>, PrometheusError> {
        let labels: HashMap<String, String> = self
            .const_labels
            .clone()
            .into_iter()
            .chain(const_labels)
            .collect();
        let opts = HistogramOpts::new(name.clone(), help)
            .const_labels(labels)
            .buckets(buckets);
        let histogram = Box::new(Histogram::with_opts(opts)?);
        self.register(name, histogram.clone());
        Ok(histogram)
    }

    fn new_histogram_vec(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        variable_labels: Vec<String>,
        buckets: Vec<f64>,
    ) -> Result<Box<HistogramVec>, PrometheusError> {
        let labels: HashMap<String, String> = self
            .const_labels
            .clone()
            .into_iter()
            .chain(const_labels)
            .collect();
        let opts = Opts::new(name.clone(), help).const_labels(labels);
        let histograms = Box::new(HistogramVec::new(
            HistogramOpts {
                common_opts: opts,
                buckets,
            },
            variable_labels
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, histograms.clone());
        Ok(histograms)
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
