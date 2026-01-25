use anyhow::{Context, Result};
// use bytes::Bytes;
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use std::str::FromStr;

use crate::broker_config::BrokerConfig;

/// Domain Message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMSMessage {
    pub id: String, 
    pub from: String,
    pub to: String,
    pub body: String,
    pub timestamp: i64,
    pub conversation_id: String,
}

// Message Broker
pub struct MessageBroker {
    producer: IggyProducer,
}

impl MessageBroker {
    pub async fn connect(
        client: Arc<IggyClient>,
        config: BrokerConfig,
    ) -> Result<Self> {
        info!("Initializing MessageBroker");

        let mut producer = client
            .producer(config.stream, config.topic)
            .context("Failed to create producer")?
            .direct(
                DirectConfig::builder()
                    .batch_length(1000)
                    .linger_time(IggyDuration::new(Duration::from_millis(1)))
                    .build(),
            )
            .partitioning(Partitioning::balanced())
            .create_stream_if_not_exists()
            .create_topic_if_not_exists(
                config.partitions,
                None,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .build();

        producer.init().await?;
        info!("✓ MessageBroker ready");

        Ok(Self { producer })
    }
   
    // Publish single SMS
    pub async fn publish_sms(&self, sms: SMSMessage) -> Result<()> {
    let payload = serde_json::to_string(&sms)
        .context("Failed to serialize SMS message to JSON string")?;

    info!(
        "Publishing SMS payload size: {} bytes",
        payload.len()
    );

    // CONSTRUCTOR FOR JSON
    let msg = IggyMessage::from_str(&payload)
        .context("Failed to build IggyMessage from string payload")?;

    self.producer.send(vec![msg]).await?;
    Ok(())
}



    
    // Publish batch SMS
 pub async fn publish_sms_batch(
    &self,
    messages: Vec<SMSMessage>,
) -> Result<()> {
    let batch = messages
        .into_iter()
        .map(|sms| {
            let payload = serde_json::to_string(&sms)?;
            info!("Publishing SMS payload size: {} bytes", payload.len());

            IggyMessage::from_str(&payload)
                .context("Failed to build IggyMessage")
        })
        .collect::<Result<Vec<_>>>()?;

    self.producer.send(batch).await?;
    Ok(())
}



}
