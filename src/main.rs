use std::{net::TcpListener, thread::sleep, time::Duration};

use anyhow::Result;
use crabmq::{
    broker::{Broker, BrokerCommandBus},
    rest::RestServer,
};
use tokio::select;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    env_logger::init();

    let broker = Broker::new();
    let (broker, bus) = broker.run();

    let rest_server = rest_server(bus.clone());

    select! {
        _ = broker => {
            log::info!("Broker stopped");
        }
        _ = rest_server.run() => {
            log::info!("REST server stopped");
        }

    }

    Ok(())
}

fn rest_server(bus: BrokerCommandBus) -> RestServer {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    RestServer::new(bus.clone(), listener)
}
