use std::error::Error;
use std::sync::Arc;

use tracing::info;

use crate::{ConversationStore, AIService, SignalWireClient};
use crate::consumers::{AIConsumer, TursoConsumer};

use conversation_store::app_config::AppConfig;
use conversation_store::infra::iggy::connect_iggy;

mod consumers;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("Starting consumer service");

    // =====================================================
    // Load + validate environment (ONCE)
    // =====================================================
    let config = Arc::new(AppConfig::load()?);

    // -----------------------------
    // Initialize Turso
    // -----------------------------
    let store = Arc::new(
        ConversationStore::new(
            config.turso_db_url.clone(),
            config.turso_auth_token.clone(),
        )
    );
    store.initialize().await?;

    // -----------------------------
    // Initialize AI + SignalWire
    // -----------------------------
    let ai_service = Arc::new(
        AIService::new(
            config.groq_model.clone(),
            config.groq_api_key.clone(),
        )
    );

    let signalwire = Arc::new(
        SignalWireClient::new(
            config.signalwire_project_id.clone(),
            config.signalwire_auth_token.clone(),
            config.signalwire_space_url.clone(),
            config.signalwire_from_number.clone(),
        )
    );

    // =====================================================
    // Dedicated Iggy client per consumer
    // =====================================================
    let turso_client = connect_iggy().await?;
    info!("✓ Turso consumer connected to Iggy");

    let ai_client = connect_iggy().await?;
    info!("✓ AI consumer connected to Iggy");

    // -----------------------------
    // Create consumers
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
