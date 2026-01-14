use anyhow::Result;
use reqwest::Client;
use serde::Serialize;

#[derive(Serialize)]
struct Message {
    #[serde(rename = "From")]
    from: String,
    #[serde(rename = "To")]
    to: String,
    #[serde(rename = "Body")]
    body: String,
}

pub struct SignalWireClient {
    client: Client,
    project_id: String,
    auth_token: String,
    space_url: String,
    from_number: String,
}

impl SignalWireClient {
    pub fn new(project_id: String, auth_token: String, space_url: String, from_number: String) -> Self {
        Self {
            client: Client::new(),
            project_id,
            auth_token,
            space_url,
            from_number,
        }
    }

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

        let response = self.client
            .post(&url)
            .basic_auth(&self.project_id, Some(&self.auth_token))
            .form(&message)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("SignalWire error {}: {}", status, text);
        }

        Ok(())
    }
}
