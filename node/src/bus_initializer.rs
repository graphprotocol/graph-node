use bus_google::GooglePubSub;
use graph::components::bus::Bus;
use graph::components::bus::BusMessage;
use graph::slog::info;
use graph::slog::warn;
use graph::tokio::sync::mpsc::unbounded_channel;
use graph::tokio::sync::mpsc::UnboundedReceiver;
use graph::tokio::sync::mpsc::UnboundedSender;
use regex::Regex;

pub struct BusInitializer;

pub enum BusScheme {
    GooglePubSub,
}

impl BusInitializer {
    fn get_bus_scheme(uri: &Option<String>) -> Option<BusScheme> {
        if uri.is_none() {
            return None;
        }

        let re = Regex::new(r"^\w+").unwrap();
        let scheme = uri.clone().and_then(|text| {
            re.find(text.as_str())
                .and_then(|regex_match| match regex_match.as_str() {
                    "pubsub" => Some(BusScheme::GooglePubSub),
                    _ => None,
                })
        });
        return scheme;
    }

    pub async fn new(
        uri: Option<String>,
        logger: graph::slog::Logger,
    ) -> (
        Option<impl Bus>,
        Option<UnboundedSender<BusMessage>>,
        Option<UnboundedReceiver<BusMessage>>,
    ) {
        let (sender, receiver) = unbounded_channel();
        match BusInitializer::get_bus_scheme(&uri) {
            Some(BusScheme::GooglePubSub) => {
                info!(logger, "Starting GooglePubSub";);
                let bus = GooglePubSub::new(uri.unwrap(), logger).await;
                (Some(bus), Some(sender), Some(receiver))
            }
            _ => {
                warn!(logger, "No bus at work";);
                (None, None, None)
            }
        }
    }
}
