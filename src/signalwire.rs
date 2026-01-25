use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
struct Message {
    #[serde(rename = "From")]
    from: String,
    #[serde(rename = "To")]
    to: String,
    #[serde(rename = "Body")]
    body: String,
}

#[derive(Clone)]
pub struct SignalWireClient {
    client: Client,
    project_id: String,
    auth_token: String,
    space_url: String,
    from_number: String,
}

impl SignalWireClient {
    /// Create client with explicit configuration
    /// (env loading is handled by AppConfig, NOT here)
    pub fn new(
        project_id: String,
        auth_token: String,
        space_url: String,
        from_number: String,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("conversation-store/sms-server")
            .build()
            .expect("Failed to build reqwest client");

        Self {
            client,
            project_id,
            auth_token,
            space_url,
            from_number,
        }
    }

    /// Send SMS via SignalWire
    pub async fn send_sms(&self, to: &str, body: &str) -> Result<()> {
        let url = format!(
            "https://{}/api/laml/2010-04-01/Accounts/{}/Messages.json",
            self.space_url, self.project_id
        );

        let message = Message {
            from: self.from_number.clone(),
            to: to.to_string(),
            body: body.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .basic_auth(&self.project_id, Some(&self.auth_token))
            .form(&message)
            .send()
            .await
            .context("Failed to send request to SignalWire")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("SignalWire error {}: {}", status, text);
        }

        Ok(())
    }
}
