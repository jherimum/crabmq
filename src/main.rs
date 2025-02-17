use std::{thread::sleep, time::Duration};

use anyhow::Result;
use crabmq::broker::Broker;
use log::{debug, info};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    env_logger::init();

    debug!("Starting broker");

    let broker = Broker::new();
    tokio::spawn(async move {
        broker.start().await;
    });

    sleep(Duration::from_secs(1));

    Ok(())
}
