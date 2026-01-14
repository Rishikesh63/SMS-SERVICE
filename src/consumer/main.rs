
use std::error::Error;
use std::sync::Arc;
use tracing::info;
use conversation_store::message_broker::MessageBroker;
use conversation_store::{ConversationStore, AIService, SignalWireClient};
mod consumers;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    // Load environment variables from .env
    dotenv::dotenv().ok();

    let broker_addr = std::env::var("BROKER_ADDR").expect("BROKER_ADDR not set");
    let turso_url = std::env::var("TURSO_URL").expect("TURSO_URL not set");
    let turso_token = std::env::var("TURSO_TOKEN").expect("TURSO_TOKEN not set");
    let ai_model = std::env::var("AI_MODEL").expect("AI_MODEL not set");
    let ai_api_key = std::env::var("AI_API_KEY").expect("AI_API_KEY not set");
    let signalwire_project = std::env::var("SIGNALWIRE_PROJECT_ID").expect("SIGNALWIRE_PROJECT_ID not set");
    let signalwire_token = std::env::var("SIGNALWIRE_AUTH_TOKEN").expect("SIGNALWIRE_AUTH_TOKEN not set");
    let signalwire_space = std::env::var("SIGNALWIRE_SPACE").expect("SIGNALWIRE_SPACE not set");
    let signalwire_from = std::env::var("SIGNALWIRE_FROM_NUMBER").expect("SIGNALWIRE_FROM_NUMBER not set");

    let broker = MessageBroker::new(&broker_addr).await?;
    let store = Arc::new(ConversationStore::new(turso_url, turso_token));
    store.initialize().await?;
    let ai_service = Arc::new(AIService::new(ai_model, ai_api_key));
    let signalwire = Arc::new(SignalWireClient::new(signalwire_project, signalwire_token, signalwire_space, signalwire_from));

    let turso_consumer = Arc::new(consumers::TursoConsumer::new(
        broker.client(),
        broker.stream_id(),
        broker.topic_id(),
        store.clone(),
    ));

    let ai_consumer = Arc::new(consumers::AIConsumer::new(
        broker.client(),
        broker.stream_id(),
        broker.topic_id(),
        store.clone(),
        ai_service.clone(),
        signalwire.clone(),
    ));

    tokio::try_join!(
        turso_consumer.start(),
        ai_consumer.start()
    )?;
    Ok(())
}



