use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};

use conversation_store::{
    app_config::AppConfig,
    consumers::{AIConsumer, TursoConsumer},
    infra::iggy::connect_iggy,
    store::ConversationStore,
    ai_service::AIService,
    signalwire::SignalWireClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("🚀 Starting consumer service");

    // =====================================================
    // Load & validate environment ONCE
    // =====================================================
    let config = Arc::new(AppConfig::load()?);

    // =====================================================
    // Initialize Turso store
    // =====================================================
    let store = Arc::new(
        ConversationStore::new(
            config.turso_db_url.clone(),
            config.turso_auth_token.clone(),
        )
    );

    store.initialize().await?;
    info!("✓ Turso initialized");

    // =====================================================
    // Initialize AI service
    // =====================================================
    let ai_service = Arc::new(
        AIService::new(
            config.groq_model.clone(),
            config.groq_api_key.clone(),
        )
    );

    // =====================================================
    // Initialize SignalWire
    // =====================================================
    let signalwire = Arc::new(
        SignalWireClient::new(
            config.signalwire_project_id.clone(),
            config.signalwire_auth_token.clone(),
            config.signalwire_space_url.clone(),
            config.signalwire_from_number.clone(),
        )
    );

    // =====================================================
    // Dedicated Iggy clients (IMPORTANT)
    // =====================================================
    let turso_client = connect_iggy().await?;
    info!("✓ Turso consumer connected to Iggy");

    let ai_client = connect_iggy().await?;
    info!("✓ AI consumer connected to Iggy");

    // =====================================================
    // Create consumers
    // =====================================================
    let turso_consumer =
        TursoConsumer::new(
            turso_client,
            store.clone(),
        )
        .await?;

    let ai_consumer =
        AIConsumer::new(
            ai_client,
            store.clone(),
            ai_service.clone(),
            signalwire.clone(),
        )
        .await?;

    info!("✓ Consumers initialized");

    // =====================================================
    // Run consumers (PARALLEL)
    // =====================================================
    tokio::try_join!(
        async {
            info!("→ Turso consumer started");
            turso_consumer.start().await
        },
        async {
            info!("→ AI consumer started");
            ai_consumer.start().await
        },
    )
    .map_err(|e| {
        error!("Consumer crashed: {e}");
        e
    })?;

    Ok(())
}
