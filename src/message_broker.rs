use anyhow::{Context, Result};
use bytes::Bytes;
use iggy::clients::client::IggyClient;
use iggy::prelude::{
    BytesSerializable,
    Client, // ✅ REQUIRED for connect()
    DirectConfig,
    IggyDuration,
    IggyMessage,
    IggyProducer,
    Partitioning,
    IggyExpiry,
    MaxTopicSize,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// -----------------------------
/// SMS Message Payload
/// -----------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMSMessage {
    pub from: String,
    pub to: String,
    pub body: String,
    pub timestamp: i64,
    pub conversation_id: String,
}

/// -----------------------------
/// Message Broker
/// -----------------------------
pub struct MessageBroker {
    producer: IggyProducer,
}

impl MessageBroker {
    /// Create a new MessageBroker
    /// NOTE: This function explicitly connects the IggyClient
    pub async fn new(client: Arc<IggyClient>) -> Result<Self> {
        // --------------------------------------------------
        // Ensure Iggy client is connected
        // --------------------------------------------------
        client
            .connect()
            .await
            .context("Failed to connect to Iggy server")?;

        info!("✓ Iggy client connected");

        // --------------------------------------------------
        // Create producer
        // --------------------------------------------------
        let mut producer = client
            .producer("sms_stream", "sms_incoming")
            .context("Failed to create SMS producer")?
            .direct(
                DirectConfig::builder()
                    .batch_length(1000)
                    .linger_time(IggyDuration::new(Duration::from_millis(1)))
                    .build(),
            )
            .create_stream_if_not_exists()
            .partitioning(Partitioning::balanced())
            .create_topic_if_not_exists(
                4, // partitions
                None,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .build();

        producer.init().await.context("Failed to init producer")?;

        info!("✓ SMS producer initialized");

        Ok(Self { producer })
    }

    /// Publish a single SMS message
    pub async fn publish_sms(&mut self, sms: SMSMessage) -> Result<()> {
        let payload = serde_json::to_vec(&sms)
            .context("Failed to serialize SMS message")?;

        let message = IggyMessage::from_bytes(Bytes::from(payload))?;
        self.producer.send(vec![message]).await?;

        Ok(())
    }

    /// Publish a batch of SMS messages
    pub async fn publish_sms_batch(&mut self, messages: Vec<SMSMessage>) -> Result<()> {
        let mut batch = Vec::with_capacity(messages.len());

        for sms in messages {
            let payload = serde_json::to_vec(&sms)
                .context("Failed to serialize SMS message")?;

            batch.push(IggyMessage::from_bytes(Bytes::from(payload))?);
        }

        self.producer.send(batch).await?;
        Ok(())
    }
}
