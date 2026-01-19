use anyhow::{Context, Result};
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

/// -----------------------------
/// AI Service (Groq / OpenAI compatible)
/// -----------------------------
pub struct AIService {
    client: Client,
    model: String,
    api_key: String,
}

impl AIService {
    pub fn new(model: String, api_key: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            model,
            api_key,
        }
    }

    /// Generate AI response given the latest user message and conversation history
    pub async fn generate_response(
        &self,
        user_message: &str,
        history: &[AIMessage],
    ) -> Result<String> {
        // Defensive: limit history size (should already be done upstream)
        let mut messages: Vec<AIMessage> = history
            .iter()
            .cloned()
            .take(20)
            .collect();

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

        // Simple retry loop for transient failures
        for attempt in 1..=2 {
            let response = self.client
                .post("https://api.groq.com/openai/v1/chat/completions")
                .header("Authorization", format!("Bearer {}", self.api_key))
                .header("Content-Type", "application/json")
                .header("User-Agent", "conversation-store/1.0")
                .json(&request)
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    let ai_response: GroqResponse = resp
                        .json()
                        .await
                        .context("Failed to parse Groq response JSON")?;

                    let content = ai_response
                        .choices
                        .first()
                        .map(|c| c.message.content.clone())
                        .ok_or_else(|| anyhow::anyhow!("No AI response choices"))?;

                    return Ok(content);
                }

                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!("Groq API error {}: {}", status, body);

                    if attempt == 2 {
                        anyhow::bail!("Groq API failed after retries: {}", status);
                    }
                }

                Err(e) => {
                    error!("Groq request failed (attempt {}): {}", attempt, e);

                    if attempt == 2 {
                        return Err(e).context("Groq request failed after retries");
                    }
                }
            }
        }

        unreachable!("Retry loop should always return");
    }
}
