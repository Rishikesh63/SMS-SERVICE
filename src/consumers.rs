use anyhow::Result;
use futures_util::StreamExt;
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::{ConversationStore, MessageRole};
use crate::ai_service::{AIMessage, AIService};
use crate::message_broker::SMSMessage;
use crate::signalwire::SignalWireClient;


/// =============================
/// Turso Consumer
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
            .consumer_group("sms-service", "sms_stream", "sms_incoming")?
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .auto_commit(AutoCommit::IntervalOrWhen(
                IggyDuration::new_from_secs(1),
                AutoCommitWhen::ConsumingAllMessages,
            ))
            .poll_interval(IggyDuration::new(Duration::from_millis(50)))
            .build();

        consumer.init().await?;
        info!("✓ Turso consumer initialized");

        Ok(Self { consumer, store })
    }

    pub async fn start(mut self) -> Result<()> {
        info!("→ Turso consumer started");

        while let Some(msg) = self.consumer.next().await {
            let sms: SMSMessage = serde_json::from_slice(&msg.unwrap().message.payload)?;

            self.store
                .store_message(
                    sms.conversation_id,
                    MessageRole::User,
                    sms.body,
                )
                .await?;
        }

        Ok(())
    }
}


/// =============================
/// AI Consumer
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
            .consumer_group("sms-ai", "sms_stream", "sms_incoming")?
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .auto_commit(AutoCommit::IntervalOrWhen(
                IggyDuration::new_from_secs(1),
                AutoCommitWhen::ConsumingAllMessages,
            ))
            .poll_interval(IggyDuration::new(Duration::from_millis(50)))
            .build();

        consumer.init().await?;
        info!("✓ AI consumer initialized");

        Ok(Self {
            consumer,
            store,
            ai,
            signalwire,
        })
    }

    pub async fn start(mut self) -> Result<()> {
        info!("→ AI consumer started");

        while let Some(msg) = self.consumer.next().await {
            let sms: SMSMessage = serde_json::from_slice(&msg.unwrap().message.payload)?;

            let history: Vec<AIMessage> = self
                .store
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
                .collect();

            let reply = self
                .ai
                .generate_response(&sms.body, &history)
                .await?;

            self.store
                .store_message(
                    sms.conversation_id.clone(),
                    MessageRole::Assistant,
                    reply.clone(),
                )
                .await?;

            self.signalwire.send_sms(&sms.from, &reply).await?;
        }

        Ok(())
    }
}
