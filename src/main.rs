use anyhow::Result;
use conversation_store::{ConversationStore, MessageRole};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let database_url = env::var("TURSO_DATABASE_URL")?;
    let auth_token = env::var("TURSO_AUTH_TOKEN")?;

    let store = ConversationStore::new(database_url, auth_token);
    store.initialize().await?;

    let conversation_id = "demo_conversation_1".to_string();

    store
        .store_message(
            conversation_id.clone(),
            MessageRole::User,
            "What is the capital of France?".to_string(),
        )
        .await?;

    store
        .store_message(
            conversation_id.clone(),
            MessageRole::Assistant,
            "Paris is the capital of France.".to_string(),
        )
        .await?;

    let messages = store
        .get_conversation_messages(&conversation_id)
        .await?;

    for msg in messages {
        println!("[{:?}] {}", msg.role, msg.content);
    }

    Ok(())
}
