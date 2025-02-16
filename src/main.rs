use anyhow::Result;
use crabmq::broker::Broker;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    env_logger::init();

    let broker = Broker::new();
    tokio::spawn(async move {
        broker.start().await;
    });

    Ok(())
}
