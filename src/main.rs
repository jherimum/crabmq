use std::{thread::sleep, time::Duration};

use anyhow::Result;
use crabmq::broker::Broker;
use tokio::select;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    env_logger::init();

    let broker = Broker::new();
    let (j, bus) = broker.run();

    select! {
        _ = j => {
            log::info!("Broker stopped");
        }
    }

    Ok(())
}
