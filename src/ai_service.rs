use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize)]
struct GroqRequest {
    model: String,
    messages: Vec<AIMessage>,
    temperature: f32,
    max_tokens: u32,
}

#[derive(Debug, Deserialize)]
struct GroqResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: AIMessage,
}

pub struct AIService {
    client: Client,
    model: String,
    api_key: String,
}

impl AIService {
    pub fn new(model: String, api_key: String) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
            model,
            api_key,
        }
    }

    pub async fn generate_response(&self, user_message: &str, history: &[AIMessage]) -> Result<String> {
        let mut messages = history.to_vec();
        messages.push(AIMessage {
            role: "user".to_string(),
            content: user_message.to_string(),
        });

        let request = GroqRequest {
            model: self.model.clone(),
            messages,
            temperature: 0.7,
            max_tokens: 500,
        };

        let response = self
            .client
            .post("https://api.groq.com/openai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!("Groq API error {}: {}", status, text);
            anyhow::bail!("AI service failed: {}", status);
        }

        let ai_response: GroqResponse = response.json().await?;
        let content = ai_response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| anyhow::anyhow!("No AI response"))?;

        Ok(content)
    }
}
