use anyhow::{Context, Result};
use bytes::Bytes;
use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, error, debug};

use crate::message_broker::SMSMessage;
use crate::ai_service::{AIMessage, AIService};
use crate::signalwire::SignalWireClient;
use crate::zero_copy::LazyMessage;
use conversation_store::{ConversationStore, MessageRole};

/// Consumer for storing SMS messages in Turso
pub struct TursoConsumer {
    client: Arc<IggyClient>,
    stream_id: u32,
    topic_id: u32,
    consumer_group_id: u32,  // Shared group ID
    consumer_member_id: u32, // Unique member ID within group
    store: Arc<ConversationStore>,
}

impl TursoConsumer {
    pub fn new(
        client: Arc<IggyClient>,
        stream_id: u32,
        topic_id: u32,
        store: Arc<ConversationStore>,
    ) -> Self {
        Self {
            client,
            stream_id,
            topic_id,
            consumer_group_id: 100,  // Unique group: Turso sees ALL messages
            consumer_member_id: 1,   // Member 1 in its own group
            store,
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("→ Turso consumer ready (group=100)");
        
        loop {
            match self.poll_and_process().await {
                Ok(_) => {}
                Err(e) => {
                    error!("Turso consumer error: {}", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn poll_and_process(&self) -> Result<()> {
        let stream_id = Identifier::numeric(self.stream_id)?;
        let topic_id = Identifier::numeric(self.topic_id)?;
        
        // Turso Consumer (Group 100, Member 1)
        let consumer = Consumer::new(Identifier::numeric(self.consumer_member_id)?);

        let polled = self.client.poll_messages(
            &stream_id,
            &topic_id,
            None, // Poll from all partitions
            &consumer,
            &PollingStrategy::next(),
            10,
            true, // auto_commit
        ).await?;

        if polled.messages.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            // ZERO-COPY OPTIMIZATION: Wrap messages in LazyMessage for deferred deserialization
            debug!("[Turso] Processing {} messages (zero-copy)", polled.messages.len());
            
            for msg in polled.messages.iter() {
                // Create zero-copy wrapper around raw bytes
                let lazy_msg = LazyMessage::new(Bytes::copy_from_slice(&msg.payload));
                
                // Deserialize only when actually needed
                let sms_view = lazy_msg.deserialize()
                    .context("Failed to deserialize SMS message")?;
                
                // Convert SMSMessageView to SMSMessage
                let sms = SMSMessage {
                    from: sms_view.from.clone(),
                    to: sms_view.to.clone(),
                    body: sms_view.body.clone(),
                    timestamp: sms_view.timestamp,
                    conversation_id: sms_view.conversation_id.clone(),
                };
                
                info!("[Turso] Storing message from {}", sms.from);

                if let Err(e) = self.process_message(sms).await {
                    error!("Failed to process message in Turso consumer: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn process_message(&self, sms: SMSMessage) -> Result<()> {

        let conversation = match self.store.get_conversation(&sms.conversation_id).await {
            Ok(Some(conv)) => conv,
            Ok(None) => {
                self.store
                    .create_conversation_with_id(
                        sms.conversation_id.clone(),
                        Some(format!("SMS: {}", sms.from))
                    )
                    .await?
            }
            Err(e) => return Err(e),
        };

        self.store
            .store_message(conversation.id, MessageRole::User, sms.body)
            .await?;

        info!("[Turso] Message stored");
        Ok(())
    }
}

/// Consumer for AI processing and response generation
pub struct AIConsumer {
    client: Arc<IggyClient>,
    stream_id: u32,
    topic_id: u32,
    consumer_group_id: u32,  // Shared group ID
    consumer_member_id: u32, // Unique member ID within group
    store: Arc<ConversationStore>,
    ai_service: Arc<AIService>,
    signalwire: Arc<SignalWireClient>,
}

impl AIConsumer {
    pub fn new(
        client: Arc<IggyClient>,
        stream_id: u32,
        topic_id: u32,
        store: Arc<ConversationStore>,
        ai_service: Arc<AIService>,
        signalwire: Arc<SignalWireClient>,
    ) -> Self {
        Self {
            client,
            stream_id,
            topic_id,
            consumer_group_id: 101,  // Unique group: AI sees ALL messages
            consumer_member_id: 1,   // Member 1 in its own group
            store,
            ai_service,
            signalwire,
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("→ AI consumer ready (group=101)");
        
        loop {
            match self.poll_and_process().await {
                Ok(_) => {}
                Err(e) => {
                    error!("AI consumer error: {}", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn poll_and_process(&self) -> Result<()> {
        let stream_id = Identifier::numeric(self.stream_id)?;
        let topic_id = Identifier::numeric(self.topic_id)?;
        
        // AI Consumer (Group 101, Member 1)
        let consumer = Consumer::new(Identifier::numeric(self.consumer_member_id)?);

        let polled = self.client.poll_messages(
            &stream_id,
            &topic_id,
            None, // Poll from all partitions
            &consumer,
            &PollingStrategy::next(),
            10,
            true, // auto_commit
        ).await?;

        if polled.messages.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            // ZERO-COPY OPTIMIZATION: Wrap messages in LazyMessage for deferred deserialization
            debug!("[AI] Processing {} messages (zero-copy)", polled.messages.len());
            
            for msg in polled.messages.iter() {
                // Create zero-copy wrapper around raw bytes
                let lazy_msg = LazyMessage::new(Bytes::copy_from_slice(&msg.payload));
                
                // Deserialize only when actually needed
                let sms_view = lazy_msg.deserialize()
                    .context("Failed to deserialize SMS message")?;
                
                // Convert SMSMessageView to SMSMessage
                let sms = SMSMessage {
                    from: sms_view.from.clone(),
                    to: sms_view.to.clone(),
                    body: sms_view.body.clone(),
                    timestamp: sms_view.timestamp,
                    conversation_id: sms_view.conversation_id.clone(),
                };
                
                info!("[AI] Processing message from {}", sms.from);

                if let Err(e) = self.process_message(sms).await {
                    error!("Failed to process message in AI consumer: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn process_message(&self, sms: SMSMessage) -> Result<()> {
        // Get conversation history
        let messages = self.store.get_conversation_messages(&sms.conversation_id).await?;
        
        let mut history: Vec<AIMessage> = messages
            .iter()
            .take(messages.len().saturating_sub(1))
            .map(|m| AIMessage {
                role: m.role.as_str().to_string(),
                content: m.content.clone(),
            })
            .collect();

        if history.len() > 10 {
            history = history.split_off(history.len() - 10);
        }

        // Generate AI response
        let ai_response = self.ai_service
            .generate_response(&sms.body, &history)
            .await
            .unwrap_or_else(|e| {
                error!("AI error: {}", e);
                "Sorry, I'm having trouble right now. Please try again later.".to_string()
            });

        info!("[AI] 💬 Response: {}", ai_response);

        // Store AI response
        let _ = self.store
            .store_message(sms.conversation_id, MessageRole::Assistant, ai_response.clone())
            .await;

        // Send SMS response
        self.signalwire.send_sms(&sms.from, &ai_response).await?;

        info!("[AI] ✓ Response sent to {}", sms.from);
        Ok(())
    }
}