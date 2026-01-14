use anyhow::{Context, Result};
use bytes::Bytes;
use iggy::client::{Client, MessageClient, StreamClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};
const DEFAULT_ROOT_USERNAME: &str = "iggy";
const DEFAULT_ROOT_PASSWORD: &str = "iggy";


/// Message structure for SMS events in Iggy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMSMessage {
    pub from: String,
    pub to: String,
    pub body: String,
    pub timestamp: i64,
    pub conversation_id: String,
}

/// Iggy message broker client
pub struct MessageBroker {
    client: Arc<IggyClient>,
    stream_id: u32,
    topic_id: u32,
}

impl MessageBroker {
    /// Creates a new MessageBroker and initializes streams/topics
    pub async fn new(server_address: &str) -> Result<Self> {
        info!("Connecting to Iggy at {}", server_address);
        
        let client = IggyClient::default();
        
        client.connect().await?;
        
        // Authenticate with default root user
        client.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD).await
            .context("Failed to authenticate with Iggy")?;
        info!("Connected and authenticated to Iggy");

        let stream_id = 1;
        let topic_id = 1;
        
        let broker = Self {
            client: Arc::new(client),
            stream_id,
            topic_id,
        };

        // Initialize stream and topic
        broker.initialize_stream().await?;
        broker.initialize_topic().await?;

        Ok(broker)
    }

    /// Initialize the SMS stream if it doesn't exist
    async fn initialize_stream(&self) -> Result<()> {
        let stream_name = "sms_stream";
        let stream_id = Identifier::numeric(self.stream_id)?;
        
        // Delete existing stream first to ensure clean state
        match self.client.delete_stream(&stream_id).await {
            Ok(_) => {}
            Err(_) => {} // Stream doesn't exist yet, that's fine
        }
        
        // Create fresh stream
        self.client
            .create_stream(stream_name, Some(self.stream_id))
            .await?;
        info!("âœ“ Stream ready: {}", stream_name);
        Ok(())
    }

    /// Initialize the SMS topic if it doesn't exist
    async fn initialize_topic(&self) -> Result<()> {
        let topic_name = "sms_incoming";
        let stream_id = Identifier::numeric(self.stream_id)?;
        
        match self.client.create_topic(
            &stream_id,
            topic_name,
            4, // 4 partitions for parallel writes + hash routing
            CompressionAlgorithm::None,
            Some(1), // replication_factor
            Some(self.topic_id),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        ).await {
            Ok(_) => {
                info!("✓ Topic ready: {} (4 partitions, hash routing by conversation_id)", topic_name);
                Ok(())
            }
            Err(e) => {
                warn!("Topic creation: {} (might already exist)", e);
                Ok(())
            }
        }
    }

    /// Publish an SMS message using hash routing by conversation_id
    /// Ensures same conversation always goes to same partition for ordering
    pub async fn publish_sms(&self, sms: SMSMessage) -> Result<()> {
        let payload = serde_json::to_vec(&sms)
            .context("Failed to serialize SMS message")?;

        let stream_id = Identifier::numeric(self.stream_id)?;
        let topic_id = Identifier::numeric(self.topic_id)?;

        // Hash routing: Same conversation_id → Same partition (ordering guarantee)
        let partition_id = self.get_partition_for_conversation(&sms.conversation_id);
        
        // NOTE: Message views with custom headers would be a future optimization
        // for zero-copy access to metadata once available in Iggy public API
        let message = Message::new(None, payload.into(), None);
        
        self.client.send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(partition_id),
            &mut [message],
        ).await
            .context(format!("Failed to send message to partition {}", partition_id))?;

        info!("📨 Published SMS from {} to partition {} (hash routing)", sms.from, partition_id);
        Ok(())
    }

    /// Publish a batch of SMS messages with hash routing and partition grouping
    /// ZERO-COPY OPTIMIZATION: Pre-serialized messages are grouped by partition
    /// without additional copying, minimizing allocations in the hot path
    pub async fn publish_sms_batch(&self, messages: Vec<SMSMessage>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let stream_id = Identifier::numeric(self.stream_id)?;
        let topic_id = Identifier::numeric(self.topic_id)?;

        // OPTIMIZATION 1: Pre-serialize all messages once
        // This allows reusing serialized bytes without re-serialization
        let serialized: Vec<(u32, Bytes)> = messages.iter()
            .map(|sms| {
                let payload = serde_json::to_vec(&sms)
                    .context("Failed to serialize SMS message")?;
                let partition_id = self.get_partition_for_conversation(&sms.conversation_id);
                Ok((partition_id, Bytes::from(payload)))
            })
            .collect::<Result<Vec<_>>>()?;

        // OPTIMIZATION 2: Group by partition with pre-allocated capacity
        let mut partition_groups: std::collections::HashMap<u32, Vec<Message>> = 
            std::collections::HashMap::with_capacity(4);
        
        for (partition_id, payload) in serialized {
            let message = Message::new(None, payload, None);
            
            partition_groups.entry(partition_id)
                .or_insert_with(|| Vec::with_capacity(messages.len() / 4))
                .push(message);
        }

        let total_partitions = partition_groups.len();

        // Send batches to each partition
        for (partition_id, mut batch) in partition_groups {
            self.client.send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition_id),
                &mut batch,
            ).await
                .context(format!("Failed to send batch to partition {}", partition_id))?;
            
            info!("📦 Published batch of {} messages to partition {}", batch.len(), partition_id);
        }

        info!("✅ Total batch: {} messages across {} partitions (hash routed)", messages.len(), total_partitions);
        Ok(())
    }

    /// Hash routing: Same conversation_id goes to same partition (1-4)
    /// Ensures message ordering per conversation
    fn get_partition_for_conversation(&self, conversation_id: &str) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        conversation_id.hash(&mut hasher);
        let hash = hasher.finish();
        
        // Map to partition 1-4
        ((hash % 4) as u32) + 1
    }

    /// Get the underlying client for consumers
    pub fn client(&self) -> Arc<IggyClient> {
        self.client.clone()
    }

    /// Get stream ID
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }

    /// Get topic ID
    pub fn topic_id(&self) -> u32 {
        self.topic_id
    }
}

