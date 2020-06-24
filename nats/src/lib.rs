use graph::prelude::{ChainHeadUpdateListener as ChainHeadUpdateListenerTrait, *};
use nats;

pub struct ChainHeadUpdateListener {
    logger: Logger,
    nc: nats::Connection,
}

impl ChainHeadUpdateListener {
    pub fn new(logger: &Logger, url: &str) -> Result<Self, Error> {
        let nc = nats::connect(url)?;
        Ok(Self {
            nc,
            logger: logger.new(o!("component" => "ChainHeadUpdateListener")),
        })
    }
}

impl ChainHeadUpdateListenerTrait for ChainHeadUpdateListener {
    fn subscribe(&self, network_name: String) -> ChainHeadUpdateStream {
        let sub = self
            .nc
            .subscribe("chain-head-updates")
            .expect("failed to subscribe to NATS (subject: `chain-head-updates`)");

        let (mut tx, rx) = futures03::channel::mpsc::channel(100);

        let logger_for_err = self.logger.clone();

        std::thread::spawn(move || {
            for msg in sub {
                match serde_json::from_slice::<ChainHeadUpdate>(&msg.data) {
                    Ok(update) => match tx.try_send(update.clone()) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!(
                                logger_for_err,
                                "Failed to forward chain head update";
                                "update" => format!("{:?}", update),
                                "error" => format!("{}", e)
                            );
                        }
                    },
                    Err(e) => {
                        warn!(
                            logger_for_err,
                            "Received invalid chain head update";
                            "error" => format!("{}", e)
                        );
                    }
                }
            }
        });

        let f = move |update: ChainHeadUpdate| {
            if update.network_name == network_name {
                futures03::future::ready(Some(()))
            } else {
                futures03::future::ready(None)
            }
        };

        return Box::new(rx.filter_map(f).map(Result::<_, ()>::Ok).boxed().compat());
    }
}
