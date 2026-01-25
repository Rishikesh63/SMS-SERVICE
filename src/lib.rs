pub mod models;
pub mod store;
pub mod ai_service;
pub mod signalwire;
pub mod zero_copy;
pub mod message_broker;
pub mod consumers;
pub mod infra;
pub mod app_config;
pub mod broker_config;

pub use models::{Conversation, Message, MessageRole};
pub use store::ConversationStore;
pub use ai_service::{AIMessage, AIService};
pub use signalwire::SignalWireClient;
use anyhow::Result;
/// Connect to a Turso database using HTTP API
pub async fn connect_turso(database_url: &str, auth_token: &str) -> Result<ConversationStore> {
    let store = ConversationStore::new(database_url.to_string(), auth_token.to_string());
    store.initialize().await?;
    Ok(store)
}
