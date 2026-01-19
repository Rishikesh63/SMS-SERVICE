use dotenvy::dotenv;
use std::error::Error;
use std::sync::Arc;
use tracing::info;
use iggy::clients::client::IggyClient;
use iggy::client::{Client, UserClient};
use crate::{ConversationStore, AIService, SignalWireClient};
use crate::consumers::{AIConsumer, TursoConsumer};
mod consumers;

/// Helper: connect + login a new Iggy client
async fn connect_iggy() -> Result<Arc<IggyClient>, Box<dyn Error>> {
    let client = Arc::new(IggyClient::default());
    client.connect().await?;
    client.login_user("iggy", "iggy").await?;
    Ok(client)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    dotenv().ok();

    info!("Starting consumer service");

    // -----------------------------
    // Load environment variables
    // -----------------------------
    let turso_url =
        std::env::var("TURSO_DATABASE_URL").expect("TURSO_DATABASE_URL not set");
    let turso_token =
        std::env::var("TURSO_AUTH_TOKEN").expect("TURSO_AUTH_TOKEN not set");

    let ai_model =
        std::env::var("GROQ_MODEL").expect("GROQ_MODEL not set");
    let ai_api_key =
        std::env::var("GROQ_API_KEY").expect("GROQ_API_KEY not set");

    let signalwire_project =
        std::env::var("SIGNALWIRE_PROJECT_ID").expect("SIGNALWIRE_PROJECT_ID not set");
    let signalwire_token =
        std::env::var("SIGNALWIRE_AUTH_TOKEN").expect("SIGNALWIRE_AUTH_TOKEN not set");
    let signalwire_space =
        std::env::var("SIGNALWIRE_SPACE_URL").expect("SIGNALWIRE_SPACE_URL not set");
    let signalwire_from =
        std::env::var("SIGNALWIRE_FROM_NUMBER").expect("SIGNALWIRE_FROM_NUMBER not set");

    // -----------------------------
    // Initialize Turso
    // -----------------------------
    let store = Arc::new(ConversationStore::new(turso_url, turso_token));
    store.initialize().await?;

    // -----------------------------
    // Initialize AI + SignalWire
    // -----------------------------
    let ai_service = Arc::new(AIService::new(ai_model, ai_api_key));
    let signalwire = Arc::new(SignalWireClient::new(
        signalwire_project,
        signalwire_token,
        signalwire_space,
        signalwire_from,
    ));

    // =====================================================
    // DEDICATED IGGY CLIENT PER CONSUMER (REQUIRED)
    // =====================================================

    let turso_client = connect_iggy().await?;
    info!("✓ Turso consumer connected to Iggy");

    let ai_client = connect_iggy().await?;
    info!("✓ AI consumer connected to Iggy");

    // -----------------------------
    // Create consumers (ConsumerGroup-based)
    // -----------------------------
    let turso_consumer =
        TursoConsumer::new(turso_client, store.clone()).await?;

    let ai_consumer =
        AIConsumer::new(
            ai_client,
            store.clone(),
            ai_service.clone(),
            signalwire.clone(),
        ).await?;

    // -----------------------------
    // Run consumers
    // -----------------------------
    tokio::try_join!(
        turso_consumer.start(),
        ai_consumer.start(),
    )?;

    Ok(())
}
