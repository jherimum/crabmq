use std::{net::TcpListener, sync::Arc};

use anyhow::Result;
use crabmq::{
    broker::{Broker, BrokerCommandBus},
    rest::RestServer,
    storage::memory::InMemryStorage,
};
use tokio::select;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    env_logger::init();

    let storage = Arc::new(InMemryStorage);
    let broker = Broker::new(storage);
    let (broker, bus) = broker.run().await?;

    //let rest_server = rest_server(bus.clone());

    select! {
        _ = broker => {
            log::info!("Broker stopped");
        }
        // _ = rest_server.run() => {
        //     log::info!("REST server stopped");
        // }

    }

    Ok(())
}

fn rest_server(bus: BrokerCommandBus) -> RestServer {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    RestServer::new(bus.clone(), listener)
}
