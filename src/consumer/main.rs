
use dotenvy::dotenv;
use std::error::Error;
use std::sync::Arc;
use tracing::info;
use conversation_store::message_broker::MessageBroker;
use conversation_store::{ConversationStore, AIService, SignalWireClient};
mod consumers;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    // Load the .env file. Use .ok() to ignore an error if the file is not found,
    // which is common in production where environment variables are set natively.
    dotenv().ok();

    let broker_addr = std::env::var("IGGY_SERVER_ADDRESS").expect("IGGY_SERVER_ADDRESS not set");
    let turso_url = std::env::var("TURSO_DATABASE_URL").expect("TURSO_DATABASE_URL not set");
    let turso_token = std::env::var("TURSO_AUTH_TOKEN").expect("TURSO_AUTH_TOKEN not set");
    let ai_model = std::env::var("GROQ_MODEL").expect("GROQ_MODEL not set");
    let ai_api_key = std::env::var("GROQ_API_KEY").expect("GROQ_API_KEY not set");
    let signalwire_project = std::env::var("SIGNALWIRE_PROJECT_ID").expect("SIGNALWIRE_PROJECT_ID not set");
    let signalwire_token = std::env::var("SIGNALWIRE_AUTH_TOKEN").expect("SIGNALWIRE_AUTH_TOKEN not set");
    let signalwire_space = std::env::var("SIGNALWIRE_SPACE_URL").expect("SIGNALWIRE_SPACE_URL not set");
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



