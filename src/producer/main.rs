use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;
use tracing::info;

use conversation_store::app_config::AppConfig;
use conversation_store::infra::iggy::connect_iggy;
use conversation_store::message_broker::{MessageBroker, SMSMessage};
use conversation_store::broker_config::BrokerConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    // Load + validate env ONCE (no dotenv here)
    let _config = AppConfig::load()?;

    info!("Starting SMS producer");

    // =====================================================
    // IGGY CLIENT
    // =====================================================
    let client = connect_iggy().await?;
    info!("✓ Connected to Iggy");

    // =====================================================
    // MESSAGE BROKER (PRODUCER ROLE)
    // =====================================================
    let broker = Arc::new(
    MessageBroker::connect(
        client.clone(),
        BrokerConfig {
            stream: "sms_stream",
            topic: "sms_incoming",
            partitions: 4,
        },
    )
    .await?
);


    // =====================================================
    // PRODUCER LOOP
    // =====================================================
    produce_sms_loop(broker).await?;

    Ok(())
}

async fn produce_sms_loop(
    broker: Arc<MessageBroker>,
) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    let mut current_id: u64 = 0;

    loop {
        current_id += 1;

        let sms = SMSMessage {
            id: uuid::Uuid::new_v4().to_string(),
            from: "+1234567890".to_string(),
            to: "+1098765432".to_string(),
            body: format!("Hello, this is message #{}", current_id),
            timestamp: chrono::Utc::now().timestamp(),
            conversation_id: format!("conv-{}", current_id % 4),
        };

        broker.publish_sms(sms).await?;

        info!("Published SMS #{}", current_id);

        sleep(interval).await;
    }
}
