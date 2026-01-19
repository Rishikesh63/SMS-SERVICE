use anyhow::Result;
use conversation_store::{connect_turso, MessageRole};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();
    
    println!("Conversation Store Example\n");

    // Load environment variables from .env file
    let database_url = env::var("TURSO_DATABASE_URL")
        .expect("TURSO_DATABASE_URL must be set in .env file");
    let auth_token = env::var("TURSO_AUTH_TOKEN")
        .expect("TURSO_AUTH_TOKEN must be set in .env file");

    // Connect to Turso database
    println!("Connecting to Turso database...");
    let store = connect_turso(&database_url, &auth_token).await?;
    println!("✓ Connected to Turso database");

    // Create a new conversation
    let conversation = store.create_conversation(Some("My AI Chat".to_string())).await?;
    println!("✓ Created conversation: {}", conversation.id);
    println!("  Title: {:?}\n", conversation.title);

    // Store a user message
    let user_msg = store
        .store_message(
            conversation.id.clone(),
            MessageRole::User,
            "What is the capital of France?".to_string(),
        )
        .await?;
    println!("✓ Stored user message: {}", user_msg.id);

    // Store an AI response
    let ai_msg = store
        .store_message(
            conversation.id.clone(),
            MessageRole::Assistant,
            "The capital of France is Paris. It's known for its iconic landmarks like the Eiffel Tower and the Louvre Museum.".to_string(),
        )
        .await?;
    println!("✓ Stored AI response: {}", ai_msg.id);

    // Store another exchange
    store
        .store_message(
            conversation.id.clone(),
            MessageRole::User,
            "Tell me more about Paris".to_string(),
        )
        .await?;

    store
        .store_message(
            conversation.id.clone(),
            MessageRole::Assistant,
            "Paris is the most populous city in France with over 2 million residents. It's a global center for art, fashion, gastronomy, and culture.".to_string(),
        )
        .await?;

    println!("\nRetrieving conversation history...\n");

    // Retrieve all messages
    let messages = store.get_conversation_messages(&conversation.id).await?;
    
    for (i, msg) in messages.iter().enumerate() {
        let role_emoji = match msg.role {
            MessageRole::User => "👤",
            MessageRole::Assistant => "🤖",
        };
        println!("{}. {} {:?}:", i + 1, role_emoji, msg.role);
        println!("   {}", msg.content);
        println!("   [{}]", msg.created_at.format("%Y-%m-%d %H:%M:%S"));
        println!();
    }

    // Get message count
    let count = store.get_message_count(&conversation.id).await?;
    println!("Total messages: {}\n", count);

    // List all conversations
    println!("All conversations:");
    let conversations = store.list_conversations().await?;
    for conv in conversations {
        println!("  - {} (ID: {})", conv.title.unwrap_or_else(|| "Untitled".to_string()), conv.id);
        println!("    Updated: {}", conv.updated_at.format("%Y-%m-%d %H:%M:%S"));
    }

    println!("\nExample completed successfully!");

    Ok(())
}
