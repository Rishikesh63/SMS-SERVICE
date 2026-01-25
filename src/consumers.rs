use anyhow::Result;
use futures_util::StreamExt;
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::{ConversationStore, MessageRole};
use crate::ai_service::{AIMessage, AIService};
use crate::message_broker::SMSMessage;
use crate::signalwire::SignalWireClient;

/// =============================
/// CONSTANTS
/// =============================
const STREAM_NAME: &str = "sms_stream";
const TOPIC_NAME: &str = "sms_incoming";

/// =============================
/// Turso Consumer (stores USER msgs)
/// =============================
pub struct TursoConsumer {
    consumer: IggyConsumer,
    store: Arc<ConversationStore>,
}

impl TursoConsumer {
    pub async fn new(
        client: Arc<IggyClient>,
        store: Arc<ConversationStore>,
    ) -> Result<Self> {
        let mut consumer = client
            .consumer_group(
                "sms-turso-consumer-group",
                STREAM_NAME,
                TOPIC_NAME,
            )?
            .auto_commit(AutoCommit::Disabled) //manual commit
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .polling_strategy(PollingStrategy::next())
            .poll_interval(IggyDuration::new(Duration::from_millis(50)))
            .build();

        consumer.init().await?;
        info!("✓ SMS Turso consumer initialized");

        Ok(Self { consumer, store })
    }

    pub async fn start(mut self) -> Result<()> {
        info!("→ SMS Turso consumer started");

        while let Some(result) = self.consumer.next().await {
            let msg = match result {
                Ok(m) => m,
                Err(e) => {
                    error!("Turso polling error: {e}");
                    continue;
                }
            };

            let offset = msg.message.header.offset;

            let sms: SMSMessage =
                serde_json::from_slice(&msg.message.payload)?;

            info!(
                "📥 User SMS | conv={} | from={} | body={}",
                sms.conversation_id,
                sms.from,
                sms.body
            );

            self.store
                .store_message(
                    sms.conversation_id,
                    MessageRole::User,
                    sms.body,
                )
                .await?;

            // ACK AFTER DB WRITE
            // self.consumer
            //     .store_offset(offset + 1, None)
            //     .await?;
        }

        Ok(())
    }
}

/// =============================
/// AI Consumer (reply + send SMS)
/// =============================
pub struct AIConsumer {
    consumer: IggyConsumer,
    store: Arc<ConversationStore>,
    ai: Arc<AIService>,
    signalwire: Arc<SignalWireClient>,
}

impl AIConsumer {
    pub async fn new(
        client: Arc<IggyClient>,
        store: Arc<ConversationStore>,
        ai: Arc<AIService>,
        signalwire: Arc<SignalWireClient>,
    ) -> Result<Self> {
        let mut consumer = client
            .consumer_group(
                "sms-ai-consumer-group",
                STREAM_NAME,
                TOPIC_NAME,
            )?
            .auto_commit(AutoCommit::Disabled) // 🔒 REQUIRED
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .polling_strategy(PollingStrategy::next())
            .poll_interval(IggyDuration::new(Duration::from_millis(50)))
            .build();

        consumer.init().await?;
        info!("✓ SMS AI consumer initialized");

        Ok(Self {
            consumer,
            store,
            ai,
            signalwire,
        })
    }

    pub async fn start(mut self) -> Result<()> {
        info!("→ SMS AI consumer started");

        while let Some(result) = self.consumer.next().await {
            let msg = match result {
                Ok(m) => m,
                Err(e) => {
                    error!("AI polling error: {e}");
                    continue;
                }
            };

            let offset = msg.message.header.offset;

            let sms: SMSMessage =
                serde_json::from_slice(&msg.message.payload)?;

            // Idempotency guard
            if self.store.is_message_processed(&sms.id).await? {
                info!("⏭️ Skipping duplicate {}", sms.id);

                self.consumer
                    .store_offset(offset + 1, None)
                    .await?;
                continue;
            }

            let history = self.store
                .get_conversation_messages(&sms.conversation_id)
                .await?
                .into_iter()
                .rev()
                .take(10)
                .rev()
                .map(|m| AIMessage {
                    role: m.role.as_str().to_string(),
                    content: m.content,
                })
                .collect::<Vec<_>>();

            let reply = self.ai
                .generate_response(&sms.body, &history)
                .await?;

            info!(
                "🤖 AI Reply | conv={} | to={} | reply={}",
                sms.conversation_id,
                sms.from,
                reply
            );

            self.store
                .store_message(
                    sms.conversation_id.clone(),
                    MessageRole::Assistant,
                    reply.clone(),
                )
                .await?;

            self.signalwire
                .send_sms(&sms.from, &reply)
                .await?;

            self.store
                .mark_message_processed(&sms.id)
                .await?;

            // FINAL ACK (THIS IS THE COMMIT)
            self.consumer
                .store_offset(offset + 1, None)
                .await?;

            info!("Reply sent & committed for {}", sms.id);
        }

        Ok(())
    }
}
