use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::models::{Conversation, Message, MessageRole};

#[derive(Debug, Serialize)]
struct TursoRequest {
    statements: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TursoResponse {
    results: Vec<QueryResult>,
}

#[derive(Debug, Deserialize)]
struct QueryResult {
    columns: Option<Vec<String>>,
    rows: Option<Vec<Vec<serde_json::Value>>>,
}

pub struct ConversationStore {
    client: Client,
    database_url: String,
    auth_token: String,
}

impl ConversationStore {
    /// Creates a new ConversationStore with HTTP API connection
    pub fn new(database_url: String, auth_token: String) -> Self {
        Self {
            client: Client::new(),
            database_url: database_url.replace("libsql://", "https://"),
            // Trim whitespace and carriage returns from the auth token
            auth_token: auth_token.trim().to_string(),
        }
    }

    async fn execute_sql(&self, sql: &str) -> Result<TursoResponse> {
        let url = format!("{}/v2/pipeline", self.database_url);
        
        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .json(&json!({
                "requests": [{"type": "execute", "stmt": {"sql": sql}}]
            }))
            .send()
            .await
            .context("Failed to send request to Turso")?;

        if !response.status().is_success() {
            let status = response.status();
            let text: String = response.text().await.unwrap_or_default();
            anyhow::bail!("Turso request failed with status {}: {}", status, text);
        }

        let json_response: serde_json::Value = response.json().await?;
        let results = json_response["results"]
            .as_array()
            .context("Invalid response format")?;

        Ok(TursoResponse {
            results: results
                .iter()
                .map(|r| QueryResult {
                    columns: r["response"]["result"]["cols"]
                        .as_array()
                        .map(|cols| {
                            cols.iter()
                                .filter_map(|c| c["name"].as_str().map(String::from))
                                .collect()
                        }),
                    rows: r["response"]["result"]["rows"]
                        .as_array()
                        .map(|rows| {
                            rows.iter()
                                .map(|row| {
                                    row.as_array()
                                        .unwrap_or(&vec![])
                                        .iter()
                                        .map(|v| v["value"].clone())
                                        .collect()
                                })
                                .collect()
                        }),
                })
                .collect(),
        })
    }

    /// Initialize the database schema
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
            "CREATE INDEX IF NOT EXISTS idx_messages_conversation_id 
             ON messages(conversation_id)",
        )
        .await?;

        Ok(())
    }

    /// Create a new conversation
    pub async fn create_conversation(&self, title: Option<String>) -> Result<Conversation> {
        let conversation = Conversation::new(title);

        let sql = format!(
            "INSERT INTO conversations (id, title, created_at, updated_at) VALUES ('{}', {}, '{}', '{}')",
            conversation.id,
            conversation.title.as_ref().map(|t| format!("'{}'", t.replace("'", "''"))).unwrap_or("NULL".to_string()),
            conversation.created_at.to_rfc3339(),
            conversation.updated_at.to_rfc3339()
        );

        self.execute_sql(&sql).await?;
        Ok(conversation)
    }

    /// Create a new conversation with a specific ID
    pub async fn create_conversation_with_id(&self, id: String, title: Option<String>) -> Result<Conversation> {
        let now = Utc::now();
        let conversation = Conversation {
            id,
            title,
            created_at: now,
            updated_at: now,
        };

        let sql = format!(
            "INSERT INTO conversations (id, title, created_at, updated_at) VALUES ('{}', {}, '{}', '{}')",
            conversation.id,
            conversation.title.as_ref().map(|t| format!("'{}'", t.replace("'", "''"))).unwrap_or("NULL".to_string()),
            conversation.created_at.to_rfc3339(),
            conversation.updated_at.to_rfc3339()
        );

        self.execute_sql(&sql).await?;
        Ok(conversation)
    }

    /// Store a new message in the database
    pub async fn store_message(
        &self,
        conversation_id: String,
        role: MessageRole,
        content: String,
    ) -> Result<Message> {
        let message = Message::new(conversation_id.clone(), role, content);

        let sql = format!(
            "INSERT INTO messages (id, conversation_id, role, content, created_at) VALUES ('{}', '{}', '{}', '{}', '{}')",
            message.id,
            message.conversation_id,
            message.role.as_str(),
            message.content.replace("'", "''"),
            message.created_at.to_rfc3339()
        );

        self.execute_sql(&sql).await?;

        // Update conversation timestamp
        let update_sql = format!(
            "UPDATE conversations SET updated_at = '{}' WHERE id = '{}'",
            Utc::now().to_rfc3339(),
            conversation_id
        );
        self.execute_sql(&update_sql).await?;

        Ok(message)
    }

    /// Retrieve all messages for a conversation
    pub async fn get_conversation_messages(&self, conversation_id: &str) -> Result<Vec<Message>> {
        let sql = format!(
            "SELECT id, conversation_id, role, content, created_at FROM messages WHERE conversation_id = '{}' ORDER BY created_at ASC",
            conversation_id
        );

        let response = self.execute_sql(&sql).await?;
        let mut messages = Vec::new();

        if let Some(result) = response.results.first() {
            if let Some(rows) = &result.rows {
                for row in rows {
                    let id = row[0].as_str().unwrap_or("").to_string();
                    let conv_id = row[1].as_str().unwrap_or("").to_string();
                    let role_str = row[2].as_str().unwrap_or("").to_string();
                    let content = row[3].as_str().unwrap_or("").to_string();
                    let created_at_str = row[4].as_str().unwrap_or("").to_string();

                    // MessageRole::from_str returns Option<MessageRole>
                    let role = MessageRole::from_str(&role_str)
                        .context(format!("Invalid role: {}", role_str))?;
                    
                    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                        .context("Failed to parse timestamp")?
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
        }

        Ok(messages)
    }

    /// Retrieve a specific conversation by ID
    pub async fn get_conversation(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        let sql = format!(
            "SELECT id, title, created_at, updated_at FROM conversations WHERE id = '{}'",
            conversation_id
        );

        let response = self.execute_sql(&sql).await?;

        if let Some(result) = response.results.first() {
            if let Some(rows) = &result.rows {
                if let Some(row) = rows.first() {
                    let id = row[0].as_str().unwrap_or("").to_string();
                    let title = row[1].as_str().map(String::from);
                    let created_at_str = row[2].as_str().unwrap_or("");
                    let updated_at_str = row[3].as_str().unwrap_or("");

                    let created_at = DateTime::parse_from_rfc3339(created_at_str)
                        .context("Failed to parse created_at")?
                        .with_timezone(&Utc);
                    let updated_at = DateTime::parse_from_rfc3339(updated_at_str)
                        .context("Failed to parse updated_at")?
                        .with_timezone(&Utc);

                    return Ok(Some(Conversation {
                        id,
                        title,
                        created_at,
                        updated_at,
                    }));
                }
            }
        }

        Ok(None)
    }

    /// List all conversations
    pub async fn list_conversations(&self) -> Result<Vec<Conversation>> {
        let sql = "SELECT id, title, created_at, updated_at FROM conversations ORDER BY updated_at DESC";
        let response = self.execute_sql(sql).await?;
        let mut conversations = Vec::new();

        if let Some(result) = response.results.first() {
            if let Some(rows) = &result.rows {
                for row in rows {
                    let id = row[0].as_str().unwrap_or("").to_string();
                    let title = row[1].as_str().map(String::from);
                    let created_at_str = row[2].as_str().unwrap_or("");
                    let updated_at_str = row[3].as_str().unwrap_or("");

                    let created_at = DateTime::parse_from_rfc3339(created_at_str)
                        .context("Failed to parse created_at")?
                        .with_timezone(&Utc);
                    let updated_at = DateTime::parse_from_rfc3339(updated_at_str)
                        .context("Failed to parse updated_at")?
                        .with_timezone(&Utc);

                    conversations.push(Conversation {
                        id,
                        title,
                        created_at,
                        updated_at,
                    });
                }
            }
        }

        Ok(conversations)
    }

    /// Delete a conversation and all its messages
    pub async fn delete_conversation(&self, conversation_id: &str) -> Result<()> {
        // First delete messages to maintain referential integrity
        let delete_messages_sql = format!("DELETE FROM messages WHERE conversation_id = '{}'", conversation_id);
        self.execute_sql(&delete_messages_sql).await?;
        
        // Then delete the conversation
        let delete_conversation_sql = format!("DELETE FROM conversations WHERE id = '{}'", conversation_id);
        self.execute_sql(&delete_conversation_sql).await?;
        
        Ok(())
    }

    /// Get the count of messages in a conversation
    pub async fn get_message_count(&self, conversation_id: &str) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = '{}'",
            conversation_id
        );

        let response = self.execute_sql(&sql).await?;

        if let Some(result) = response.results.first() {
            if let Some(rows) = &result.rows {
                if let Some(row) = rows.first() {
                    if let Some(count) = row[0].as_i64() {
                        return Ok(count);
                    }
                }
            }
        }

        Ok(0)
    }
}