use anyhow::Result;
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use std::{env, sync::Arc};

pub async fn connect_iggy() -> Result<Arc<IggyClient>> {
    let addr = env::var("IGGY_SERVER_ADDRESS")
        .unwrap_or_else(|_| "iggy-server:8090".to_string());

    let conn_str = format!("iggy://iggy:iggy@{}", addr);

    let client = IggyClient::from_connection_string(&conn_str)?;
    client.connect().await?;
    client.login_user("iggy", "iggy").await?;

    Ok(Arc::new(client))
}
