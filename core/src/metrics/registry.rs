use std::collections::HashMap;
use std::sync::Arc;

use graph::prelude::{MetricsRegistry as MetricsRegistryTrait, *};

pub struct MetricsRegistry {
    logger: Logger,
    registry: Arc<Registry>,
    const_labels: HashMap<String, String>,
    registration_errors_counter: Box<Counter>,
}

impl MetricsRegistry {
    pub fn new(logger: Logger, registry: Arc<Registry>) -> Self {
        let const_labels = HashMap::new();

        // Generate counter for internal usage
        let registration_errors_counter = {
            let opts = Opts::new(
                String::from("metrics_registration_errors"),
                String::from("Counts Prometheus metrics registration errors."),
            );
            let counter = Counter::with_opts(opts)
                .expect("failed to create `metrics_registration_errors` counter");
            Box::new(counter)
        };

        MetricsRegistry {
            logger: logger.new(o!("component" => String::from("MetricsRegistry"))),
            registry,
            const_labels,
            registration_errors_counter,
        }
    }

    pub fn register(&self, name: String, c: Box<dyn Collector>) {
        let err = match self.registry.register(c).err() {
            None => return,
            Some(err) => err,
        };
        self.registration_errors_counter.inc();
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
            registration_errors_counter: self.registration_errors_counter.clone(),
        };
    }
}

impl MetricsRegistryTrait for MetricsRegistry {
    fn new_gauge(
        &self,
        name: String,
        help: String,
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
        name: String,
        help: String,
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
        name: String,
        help: String,
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

    fn new_counter_vec(
        &self,
        name: String,
        help: String,
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
        name: String,
        help: String,
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
        name: String,
        help: String,
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
            Ok(_) => (),
            Err(e) => error!(self.logger, "Unregistering metric failed = {:?}", e,),
        };
    }
}
