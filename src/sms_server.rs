use anyhow::Result;
use axum::{
    extract::{Form, State, Path, Json},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, delete},
    Router,
};
use conversation_store::{connect_turso, ConversationStore};
use serde::Deserialize;
use std::{env, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing::{info, error};
use chrono::Utc;

mod ai_service;
mod signalwire;
mod message_broker;
mod consumers;
mod zero_copy;

use ai_service::AIService;
use signalwire::SignalWireClient;
use message_broker::{MessageBroker, SMSMessage};
use consumers::{TursoConsumer, AIConsumer};
use tokio::sync::Mutex;
use std::time::Duration;

/// Message batcher that buffers SMS messages for efficient batch processing
struct MessageBatcher {
    broker: Arc<MessageBroker>,
    buffer: Arc<Mutex<Vec<SMSMessage>>>,
    batch_size: usize,
    batch_timeout: Duration,
}

impl MessageBatcher {
    fn new(broker: Arc<MessageBroker>, batch_size: usize, batch_timeout_ms: u64) -> Arc<Self> {
        let batcher = Arc::new(Self {
            broker,
            buffer: Arc::new(Mutex::new(Vec::new())),
            batch_size,
            batch_timeout: Duration::from_millis(batch_timeout_ms),
        });

        // Spawn background task to flush batches on timeout
        let batcher_clone = batcher.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(batcher_clone.batch_timeout).await;
                if let Err(e) = batcher_clone.flush_if_needed().await {
                    error!("Failed to flush batch: {}", e);
                }
            }
        });

        batcher
    }

    /// Add a message to the buffer and flush if batch size is reached
    async fn add_message(&self, message: SMSMessage) -> Result<()> {
        let should_flush;
        let batch;
        
        {
            let mut buffer = self.buffer.lock().await;
            buffer.push(message);
            
            // Check if we've reached batch size
            should_flush = buffer.len() >= self.batch_size;
            
            if should_flush {
                batch = std::mem::take(&mut *buffer);
            } else {
                return Ok(()); // Early return while still holding lock
            }
        } // Lock released here
        
        // Perform I/O outside of lock
        if should_flush {
            info!("🔄 Flushing batch (size threshold): {} messages", batch.len());
            self.broker.publish_sms_batch(batch).await?;
        }
        
        Ok(())
    }

    /// Flush the buffer if it contains messages
    async fn flush_if_needed(&self) -> Result<()> {
        let batch = {
            let mut buffer = self.buffer.lock().await;
            
            if buffer.is_empty() {
                return Ok(()); // Early return if empty
            }
            
            std::mem::take(&mut *buffer)
        }; // Lock released here
        
        // Perform I/O outside of lock
        info!("Flushing batch (timeout): {} messages", batch.len());
        self.broker.publish_sms_batch(batch).await?;
        
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    batcher: Arc<MessageBatcher>,
    store: Arc<ConversationStore>,
}

#[derive(Debug, Deserialize)]
struct IncomingSMS {
    #[serde(rename = "From")]
    from: String,
    #[serde(rename = "To")]
    to: String,
    #[serde(rename = "Body")]
    body: String,
}

async fn health_check() -> impl IntoResponse {
    "SMS Server is running"
}

// --- Conversation API Handlers ---
use conversation_store::models::{Conversation, Message, MessageRole};

// List all conversations
async fn list_conversations(State(state): State<AppState>) -> Result<Json<Vec<Conversation>>, StatusCode> {
    state.store.list_conversations().await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// Create a new conversation
#[derive(Deserialize)]
struct CreateConversationReq { title: Option<String> }
async fn create_conversation(State(state): State<AppState>, Json(req): Json<CreateConversationReq>) -> Result<Json<Conversation>, StatusCode> {
    state.store.create_conversation(req.title).await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// Get a conversation by ID
async fn get_conversation(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Conversation>, StatusCode> {
    match state.store.get_conversation(&id).await {
        Ok(Some(conv)) => Ok(Json(conv)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// Delete a conversation
async fn delete_conversation(State(state): State<AppState>, Path(id): Path<String>) -> Result<StatusCode, StatusCode> {
    state.store.delete_conversation(&id).await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// List messages in a conversation
async fn list_messages(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Vec<Message>>, StatusCode> {
    state.store.get_conversation_messages(&id).await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// Post a message to a conversation
#[derive(Deserialize)]
struct PostMessageReq { role: MessageRole, content: String }
async fn post_message(State(state): State<AppState>, Path(id): Path<String>, Json(req): Json<PostMessageReq>) -> Result<Json<Message>, StatusCode> {
    state.store.store_message(id, req.role, req.content).await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_incoming_sms(
    State(state): State<AppState>,
    Form(sms): Form<IncomingSMS>,
) -> Result<impl IntoResponse, StatusCode> {
    // Optimized: Reduce allocations by doing single replace operation
    let conversation_id = if sms.from.contains('+') || sms.from.contains(' ') {
        format!("sms_{}", sms.from.replace(&['+', ' '][..], ""))
    } else {
        format!("sms_{}", sms.from)
    };
    
    let timestamp = Utc::now().timestamp();
    
    let sms_message = SMSMessage {
        from: sms.from,
        to: sms.to,
        body: sms.body,
        timestamp,
        conversation_id,
    };

    // Add to batch buffer - non-blocking, optimized path
    if let Err(e) = state.batcher.add_message(sms_message).await {
        error!("Failed to add message to batch: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Return immediately - static response, no allocations
    Ok((StatusCode::OK, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response></Response>"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    info!("Starting SMS Server with Iggy Message Broker...");
    dotenvy::dotenv().ok();

    let database_url = env::var("TURSO_DATABASE_URL")?;
    let auth_token = env::var("TURSO_AUTH_TOKEN")?;
    let groq_api_key = env::var("GROQ_API_KEY")?;
    let ai_model = env::var("GROQ_MODEL")
        .unwrap_or_else(|_| "llama-3.3-70b-versatile".to_string());
    let sw_project_id = env::var("SIGNALWIRE_PROJECT_ID")?;
    let sw_auth_token = env::var("SIGNALWIRE_AUTH_TOKEN")?;
    let sw_space_url = env::var("SIGNALWIRE_SPACE_URL")?;
    let sw_from_number = env::var("SIGNALWIRE_FROM_NUMBER")?;
    let port = env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let iggy_address = env::var("IGGY_SERVER_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:8090".to_string());

    // Initialize Turso store
    let store = Arc::new(connect_turso(&database_url, &auth_token).await?);
    
    // Initialize AI service
    let ai_service = Arc::new(AIService::new(ai_model.clone(), groq_api_key));
    
    // Initialize SignalWire client
    let signalwire = Arc::new(SignalWireClient::new(
        sw_project_id,
        sw_auth_token,
        sw_space_url,
        sw_from_number.clone(),
    ));

    // Initialize Iggy message broker
    let broker = Arc::new(MessageBroker::new(&iggy_address).await?);
    info!("✓ Iggy connected ({})", iggy_address);


    // Start consumers for parallel processing, each with its own IggyClient
    let turso_consumer = Arc::new(TursoConsumer::new(
        iggy::clients::client::IggyClient::default(),
        broker.stream_id(),
        broker.topic_id(),
        store.clone(),
    ));
    tokio::spawn({
        let consumer = turso_consumer.clone();
        async move {
            if let Err(e) = consumer.start().await {
                error!("Turso consumer failed: {}", e);
            }
        }
    });

    let ai_consumer = Arc::new(AIConsumer::new(
        iggy::clients::client::IggyClient::default(),
        broker.stream_id(),
        broker.topic_id(),
        store.clone(),
        ai_service.clone(),
        signalwire.clone(),
    ));
    tokio::spawn({
        let consumer = ai_consumer.clone();
        async move {
            if let Err(e) = consumer.start().await {
                error!("AI consumer failed: {}", e);
            }
        }
    });

    info!("✓ Consumer groups ready (2 active consumers)");

    // Initialize message batcher with optimized parameters
    let batch_size = env::var("BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25); // Optimized: increased from 10 to 25 for better throughput
    let batch_timeout_ms = env::var("BATCH_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2); // Optimized: reduced from 5ms to 2ms for lower P99 latency
    
    let batcher = MessageBatcher::new(broker.clone(), batch_size, batch_timeout_ms);
    info!("🔄 Message batcher ready (size: {}, timeout: {}ms)", batch_size, batch_timeout_ms);


    let state = AppState {
        batcher,
        store: store.clone(),
    };


    let api = Router::new()
        .route("/conversations", get(list_conversations).post(create_conversation))
        .route("/conversations/{id}", get(get_conversation).delete(delete_conversation))
        .route("/conversations/{id}/messages", get(list_messages).post(post_message));

    let app = Router::new()
        .route("/", get(health_check))
        .route("/health", get(health_check))
        .route("/sms/webhook", post(handle_incoming_sms))
        .nest("/api", api)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("🚀 Server ready on {}", addr);
    info!("📞 SMS: {} → AI: {}", sw_from_number, ai_model);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
