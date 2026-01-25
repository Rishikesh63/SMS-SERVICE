use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::models::{Conversation, Message, MessageRole};

/// =============================
/// Turso HTTP Types
/// =============================
#[derive(Debug, Serialize)]
struct TursoRequest {
    requests: Vec<TursoExecute>,
}

#[derive(Debug, Serialize)]
struct TursoExecute {
    #[serde(rename = "type")]
    kind: &'static str,
    stmt: TursoStatement,
}

#[derive(Debug, Serialize)]
struct TursoStatement {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct TursoResponse {
    results: Vec<TursoResult>,
}

#[derive(Debug, Deserialize)]
struct TursoResult {
    response: Option<TursoInnerResponse>,
}

#[derive(Debug, Deserialize)]
struct TursoInnerResponse {
    result: Option<TursoQueryResult>,
}

#[derive(Debug, Deserialize)]
struct TursoQueryResult {
    rows: Option<Vec<Vec<TursoValue>>>,
}

#[derive(Debug, Deserialize)]
struct TursoValue {
    value: serde_json::Value,
}

/// =============================
/// Conversation Store
/// =============================
pub struct ConversationStore {
    client: Client,
    database_url: String,
    auth_token: String,
}

impl ConversationStore {
    /// Create store (HTTP API)
    pub fn new(database_url: String, auth_token: String) -> Self {
        Self {
            client: Client::new(),
            database_url: database_url.replace("libsql://", "https://"),
            auth_token: auth_token.trim().to_string(),
        }
    }

    /// -----------------------------
    /// Low-level SQL executor
    /// -----------------------------
    async fn execute_sql(&self, sql: &str) -> Result<TursoResponse> {
        let url = format!("{}/v2/pipeline", self.database_url);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .json(&TursoRequest {
                requests: vec![TursoExecute {
                    kind: "execute",
                    stmt: TursoStatement {
                        sql: sql.to_string(),
                    },
                }],
            })
            .send()
            .await
            .context("Failed to send request to Turso")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Turso error {}: {}", status, text);
        }

        Ok(response.json::<TursoResponse>().await?)
    }

    /// -----------------------------
    /// Initialize schema
    /// -----------------------------
    pub async fn initialize(&self) -> Result<()> {
        self.execute_sql(
            "CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                title TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
        )
        .await?;

        self.execute_sql(
            "CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .await?;

        self.execute_sql(
            "CREATE TABLE IF NOT EXISTS processed_messages (
                message_id TEXT PRIMARY KEY
            )",
        )
        .await?;

        Ok(())
    }

    /// =============================
    /// IDEMPOTENCY (CRITICAL)
    /// =============================
    pub async fn is_message_processed(&self, message_id: &str) -> Result<bool> {
        let sql = format!(
            "SELECT 1 FROM processed_messages WHERE message_id = '{}' LIMIT 1",
            message_id.replace("'", "''")
        );

        let response = self.execute_sql(&sql).await?;

        Ok(response
            .results
            .get(0)
            .and_then(|r| r.response.as_ref())
            .and_then(|r| r.result.as_ref())
            .and_then(|r| r.rows.as_ref())
            .map(|rows| !rows.is_empty())
            .unwrap_or(false))
    }

    pub async fn mark_message_processed(&self, message_id: &str) -> Result<()> {
        let sql = format!(
            "INSERT OR IGNORE INTO processed_messages (message_id)
             VALUES ('{}')",
            message_id.replace("'", "''")
        );

        self.execute_sql(&sql).await?;
        Ok(())
    }

    /// -----------------------------
    /// Store message
    /// -----------------------------
    pub async fn store_message(
        &self,
        conversation_id: String,
        role: MessageRole,
        content: String,
    ) -> Result<Message> {
        let message = Message::new(conversation_id.clone(), role, content);

        let sql = format!(
            "INSERT INTO messages (id, conversation_id, role, content, created_at)
             VALUES ('{}', '{}', '{}', '{}', '{}')",
            message.id,
            message.conversation_id,
            message.role.as_str(),
            message.content.replace("'", "''"),
            message.created_at.to_rfc3339()
        );

        self.execute_sql(&sql).await?;

        let update_sql = format!(
            "UPDATE conversations
             SET updated_at = '{}'
             WHERE id = '{}'",
            Utc::now().to_rfc3339(),
            conversation_id
        );

        self.execute_sql(&update_sql).await?;
        Ok(message)
    }

    /// -----------------------------
    /// Get conversation history
    /// -----------------------------
    pub async fn get_conversation_messages(
        &self,
        conversation_id: &str,
    ) -> Result<Vec<Message>> {
        let sql = format!(
            "SELECT id, conversation_id, role, content, created_at
             FROM messages
             WHERE conversation_id = '{}'
             ORDER BY created_at ASC",
            conversation_id
        );

        let response = self.execute_sql(&sql).await?;
        let mut messages = Vec::new();

        if let Some(result) = response.results.get(0)
            .and_then(|r| r.response.as_ref())
            .and_then(|r| r.result.as_ref())
            .and_then(|r| r.rows.as_ref())
        {
            for row in result {
                let id = row[0].value.as_str().unwrap_or("").to_string();
                let conv_id = row[1].value.as_str().unwrap_or("").to_string();
                let role_str = row[2].value.as_str().unwrap_or("");
                let content = row[3].value.as_str().unwrap_or("").to_string();
                let created_at_str = row[4].value.as_str().unwrap_or("");

                let role = MessageRole::from_str(role_str)
                    .context("Invalid role")?;

                let created_at = DateTime::parse_from_rfc3339(created_at_str)?
                    .with_timezone(&Utc);

                messages.push(Message {
                    id,
                    conversation_id: conv_id,
                    role,
                    content,
                    created_at,
                });
            }
        }

        Ok(messages)
    }
}
