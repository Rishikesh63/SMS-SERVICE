use dotenvy::dotenv;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use iggy::clients::client::IggyClient;
use conversation_store::message_broker::{MessageBroker, SMSMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    dotenv().ok();

    info!("Starting SMS producer");

    // =====================================================
    // IGGY CLIENT (HIGH-LEVEL SDK)
    // =====================================================
    let client = Arc::new(
        IggyClient::from_connection_string(
            "iggy://iggy:iggy@127.0.0.1:8090"
        )?
    );

    info!("✓ Connected to Iggy");

    // =====================================================
    // MESSAGE BROKER (PRODUCER ROLE)
    // =====================================================
    let mut broker = MessageBroker::new(client.clone()).await?;

    // =====================================================
    // PRODUCER LOOP
    // =====================================================
    produce_sms_loop(&mut broker).await?;

    Ok(())
}

async fn produce_sms_loop(
    broker: &mut MessageBroker,
) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    let mut current_id: u64 = 0;

    loop {
        current_id += 1;

        let sms = SMSMessage {
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
