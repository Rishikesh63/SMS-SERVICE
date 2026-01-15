use dotenvy::dotenv;
use std::error::Error;

use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use conversation_store::message_broker::{MessageBroker, SMSMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    // Load the .env file. Use .ok() to ignore an error if the file is not found,
    // which is common in production where environment variables are set natively.
    dotenv().ok();
    // Example: Use localhost, or make configurable
    let broker = MessageBroker::new("127.0.0.1:8090").await?;
    produce_sms_loop(&broker).await?;
    Ok(())
}


async fn produce_sms_loop(broker: &MessageBroker) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    let mut current_id = 0;
    loop {
        current_id += 1;
        // Example SMS message, replace with real webhook/SMS input
        let sms = SMSMessage {
            from: "+1234567890".to_string(),
            to: "+1098765432".to_string(),
            body: format!("Hello, this is message #{}", current_id),
            timestamp: chrono::Utc::now().timestamp(),
            conversation_id: format!("conv-{}", current_id % 4), // Simulate 4 conversations
        };
        broker.publish_sms(sms).await?;
        info!("Published SMS #{}", current_id);
        sleep(interval).await;
    }
}
