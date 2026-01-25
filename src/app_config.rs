use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    // --- Server ---
    pub port: String,

    // --- Turso ---
    pub turso_db_url: String,
    pub turso_auth_token: String,

    // --- AI ---
    pub groq_model: String,
    pub groq_api_key: String,

    // --- SignalWire ---
    pub signalwire_project_id: String,
    pub signalwire_auth_token: String,
    pub signalwire_space_url: String,
    pub signalwire_from_number: String,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        // dotenv belongs HERE, nowhere else
        dotenvy::dotenv().ok();

        Ok(Self {
            port: env::var("PORT").unwrap_or_else(|_| "3001".into()),

            turso_db_url: env::var("TURSO_DATABASE_URL")
                .context("TURSO_DATABASE_URL missing")?,
            turso_auth_token: env::var("TURSO_AUTH_TOKEN")
                .context("TURSO_AUTH_TOKEN missing")?,

            groq_model: env::var("GROQ_MODEL")
                .unwrap_or_else(|_| "llama-3.3-70b-versatile".into()),
            groq_api_key: env::var("GROQ_API_KEY")
                .context("GROQ_API_KEY missing")?,

            signalwire_project_id: env::var("SIGNALWIRE_PROJECT_ID")
                .context("SIGNALWIRE_PROJECT_ID missing")?,
            signalwire_auth_token: env::var("SIGNALWIRE_AUTH_TOKEN")
                .context("SIGNALWIRE_AUTH_TOKEN missing")?,
            signalwire_space_url: env::var("SIGNALWIRE_SPACE_URL")
                .context("SIGNALWIRE_SPACE_URL missing")?,
            signalwire_from_number: env::var("SIGNALWIRE_FROM_NUMBER")
                .context("SIGNALWIRE_FROM_NUMBER missing")?,
        })
    }
}
