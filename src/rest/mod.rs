use std::net::TcpListener;
use actix_web::{dev::Server, App, HttpServer};
use crate::broker::BrokerCommandBus;

pub struct RestServer {
    command_bus: BrokerCommandBus,
    server: Server,
}

impl RestServer {
    pub fn new(command_bus: BrokerCommandBus, listener: TcpListener) -> Self {
        let server = HttpServer::new(|| App::new())
            .listen(listener)
            .unwrap()
            .run();
        RestServer {
            command_bus,
            server,
        }
    }

    pub async fn run(self) -> std::io::Result<()> {
        self.server.await
    }
}
