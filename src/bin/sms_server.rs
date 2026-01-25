use anyhow::Result;
use axum::{
    extract::{Form, State},
    routing::{get, post},
    Router,
    http::StatusCode,
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use chrono::Utc;
use serde::Deserialize;

use conversation_store::message_broker::{MessageBroker, SMSMessage};
use conversation_store::infra::iggy::connect_iggy;
use conversation_store::app_config::AppConfig;
use conversation_store::broker_config::BrokerConfig;

/// -----------------------------
/// Health
/// -----------------------------
async fn health() -> &'static str {
    "OK"
}
/// -----------------------------
/// Incoming SMS
/// -----------------------------
#[derive(Debug, Deserialize)]
struct IncomingSMS {
    #[serde(rename = "From")]
    from: String,
    #[serde(rename = "To")]
    to: String,
    #[serde(rename = "Body")]
    body: String,
}

/// -----------------------------
/// App State
/// -----------------------------
#[derive(Clone)]
struct AppState {
    broker: Arc<MessageBroker>,
}

/// -----------------------------
/// SMS Webhook
/// -----------------------------
async fn sms_webhook(
    State(state): State<AppState>,
    Form(sms): Form<IncomingSMS>,
) -> Result<StatusCode, StatusCode> {
    info!("SMS from {} → {}", sms.from, sms.body);

    let msg = SMSMessage {
         id: uuid::Uuid::new_v4().to_string(),
        from: sms.from.clone(),
        to: sms.to,
        body: sms.body,
        timestamp: Utc::now().timestamp(),
        conversation_id: format!("sms_{}", uuid::Uuid::new_v4()),
    };

    state
        .broker
        .publish_sms(msg)
        .await
        .map_err(|e| {
            error!("Failed to publish SMS: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// -----------------------------
/// MAIN
/// -----------------------------
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // Load + validate env ONCE
    let config = Arc::new(AppConfig::load()?);

    info!("Starting SMS Server");

    // -----------------------------
    // IGGY
    // -----------------------------
    let iggy = connect_iggy().await?;
    info!("✓ Connected to Iggy");

    let broker = Arc::new(
    MessageBroker::connect(
        iggy.clone(),
        BrokerConfig {
            stream: "sms_stream",
            topic: "sms_incoming",
            partitions: 4,
        },
    )
    .await?
);

    info!("✓ MessageBroker ready");

    // -----------------------------
    // HTTP SERVER
    // -----------------------------
    let app = Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        .route("/sms/webhook", post(sms_webhook))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState { broker });

    let addr = format!("0.0.0.0:{}", config.port);
    info!("Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
