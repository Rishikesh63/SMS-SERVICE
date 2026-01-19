use anyhow::Result;
use axum::{routing::get, Router};
use std::{env, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use conversation_store::connect_turso;
use conversation_store::ai_service::AIService;
use conversation_store::signalwire::SignalWireClient;
use conversation_store::consumers::{AIConsumer, TursoConsumer};

/// -----------------------------
/// Health
/// -----------------------------
async fn health() -> &'static str {
    "OK"
}

/// -----------------------------
/// Helper: create Iggy client
/// -----------------------------
fn iggy_client() -> Result<Arc<IggyClient>> {
    let addr = env::var("IGGY_SERVER_ADDRESS")
        .unwrap_or_else(|_| "iggy-server:8090".to_string());

    let conn_str = format!("iggy://iggy:iggy@{}", addr);
    info!("Connecting to Iggy: {}", conn_str);

    let client = IggyClient::from_connection_string(&conn_str)
        .map_err(|e| anyhow::anyhow!("Failed to create Iggy client: {:?}", e))?;

    Ok(Arc::new(client))
}

/// -----------------------------
/// MAIN
/// -----------------------------
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    dotenvy::dotenv().ok();

    info!("Starting SMS Server");

    // --------------------------------------------------
    // ENV
    // --------------------------------------------------
    let db_url = env::var("TURSO_DATABASE_URL")?;
    let db_token = env::var("TURSO_AUTH_TOKEN")?;
    let groq_key = env::var("GROQ_API_KEY")?;
    let model = env::var("GROQ_MODEL")
        .unwrap_or_else(|_| "llama-3.3-70b-versatile".to_string());

    let port = env::var("PORT").unwrap_or_else(|_| "3001".into());

    // --------------------------------------------------
    // STORE
    // --------------------------------------------------
    let store = Arc::new(connect_turso(&db_url, &db_token).await?);

    // --------------------------------------------------
    // AI + SIGNALWIRE
    // --------------------------------------------------
    let ai = Arc::new(AIService::new(model, groq_key));
    let signalwire = Arc::new(SignalWireClient::new(
        env::var("SIGNALWIRE_PROJECT_ID")?,
        env::var("SIGNALWIRE_AUTH_TOKEN")?,
        env::var("SIGNALWIRE_SPACE_URL")?,
        env::var("SIGNALWIRE_FROM_NUMBER")?,
    ));

    // --------------------------------------------------
    // IGGY CLIENTS
    // --------------------------------------------------
    let producer_client = iggy_client()?;
    let turso_client = iggy_client()?;
    let ai_client = iggy_client()?;

    // ==================================================
    // PRODUCER (SMS INGEST)
    // ==================================================
    let producer = producer_client
        .producer("sms_stream", "sms_incoming")?
        .direct(
            DirectConfig::builder()
                .batch_length(1000)
                .linger_time(IggyDuration::new(std::time::Duration::from_millis(1)))
                .build(),
        )
        .create_stream_if_not_exists()
        .partitioning(Partitioning::balanced())
        .create_topic_if_not_exists(
            4,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .build();
        

    info!("✓ SMS producer ready");

    // ==================================================
    // TURSO CONSUMER (persist messages)
    // ==================================================
    let turso_consumer = TursoConsumer::new(
        turso_client.clone(),
        store.clone(),
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = turso_consumer.start().await {
            error!("Turso consumer failed: {e}");
        }
    });

    // ==================================================
    // AI CONSUMER (generate reply + send SMS)
    // ==================================================
    let ai_consumer = AIConsumer::new(
        ai_client.clone(),
        store.clone(),
        ai.clone(),
        signalwire.clone(),
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = ai_consumer.start().await {
            error!("AI consumer failed: {e}");
        }
    });

    // ==================================================
    // HTTP SERVER
    // ==================================================
    let app = Router::new()
        .route("/health", get(health))
        .layer(TraceLayer::new_for_http());

    let addr = format!("0.0.0.0:{port}");
    info!("📡 Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
