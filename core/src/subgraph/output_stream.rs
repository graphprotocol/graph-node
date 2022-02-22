use fluvio::{self, metadata::topic::TopicSpec, FluvioError, RecordKey};
use graph::{
    blockchain::BlockPtr,
    prelude::{DeploymentHash, EntityModification, Error},
};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    RevertBlock {
        block: BlockPtr,
    },
    EntityModification {
        block: BlockPtr,
        modification: EntityModification,
    },
}

pub struct SubgraphOutputStream {
    producer: fluvio::TopicProducer,
}

impl SubgraphOutputStream {
    pub async fn new(deployment: DeploymentHash) -> Result<Self, Error> {
        let topic = deployment.as_str().to_lowercase();

        // Ensure that the topic exists so we can write events to it
        let admin = fluvio::FluvioAdmin::connect()
            .await
            .expect("failed to connect to fluvio admin interface");
        match admin
            .create(topic.clone(), false, TopicSpec::new_computed(1, 1, None))
            .await
        {
            Err(FluvioError::AdminApi(fluvio_sc_schema::ApiError::Code(
                fluvio_sc_schema::errors::ErrorCode::TopicAlreadyExists,
                _,
            ))) => {}
            Err(err) => panic!("failed to ensure topic: {}", err),
            _ => {}
        }

        // Connect to fluvio
        let fluvio = fluvio::Fluvio::connect()
            .await
            .expect("Failed to connect to fluvio");

        let producer = fluvio
            .topic_producer(topic.as_str())
            .await
            .expect("Failed to create a producer");

        Ok(Self { producer })
    }

    async fn send(&self, event: Event) -> Result<(), Error> {
        self.producer
            .send(RecordKey::NULL, serde_json::to_string(&event)?)
            .await?;
        Ok(())
    }

    pub async fn write_entity_modification(
        &self,
        block: BlockPtr,
        modification: EntityModification,
    ) -> Result<(), Error> {
        self.send(Event::EntityModification {
            block,
            modification,
        })
        .await
    }

    pub async fn write_revert_block(&self, block: BlockPtr) -> Result<(), Error> {
        self.send(Event::RevertBlock { block }).await
    }
}
