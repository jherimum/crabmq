use std::str::FromStr;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use log::error;
use log::info;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::time::sleep;
use tokio::{net::TcpListener, sync::Mutex};

pub mod error;

pub type Topics = Arc<Mutex<HashMap<String, Topic>>>;

pub struct Topic {
    sender: Sender<String>,
    receivers: Vec<Receiver<String>>,
}

impl Topic {
    pub fn new() -> Self {
        let (sender, rc) = channel(100);
        Self {
            sender,
            receivers: vec![rc],
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:8013").await?;
    let topics = Topics::default();

    info!("Starting broker...");

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                Connection::new(socket, topics.clone()).start().await;
            }
            Err(e) => error!("Failed: {e}"),
        }
    }
}

pub struct Connection {
    socket: TcpStream,
    topics: Topics,
}

impl Connection {
    pub fn new(socket: TcpStream, topics: Topics) -> Self {
        Connection { socket, topics }
    }

    pub async fn start(self) {
        tokio::spawn(async move {
            let (reader, writer) = self.socket.into_split();

            let mut buffer = BufReader::new(reader);
            let mut line = String::new();
            let w = Arc::new(Mutex::new(writer));

            while buffer.read_line(&mut line).await.unwrap() > 0 {
                match Command::from_str(&line) {
                    Ok(command) => {
                        let _ = command.execute(self.topics.clone(), w.clone()).await;
                    }
                    Err(e) => error!("Failed to parse command: {e}"),
                };

                line.clear();
            }
        });
    }
}

#[derive(Debug)]
pub enum Command {
    Pub(String, String),
    Sub(String),
}

impl Command {
    pub async fn execute(
        &self,
        topics: Topics,
        writer: Arc<Mutex<OwnedWriteHalf>>,
    ) -> Result<(), crate::error::Error> {
        info!("execute");
        match self {
            Command::Pub(topic_name, data) => {
                let mut topic_guard = topics.lock().await;
                let topic = topic_guard
                    .entry(topic_name.to_owned())
                    .or_insert_with(|| Topic::new());

                match topic.sender.send(data.to_owned()) {
                    Ok(_) => info!("Message sent: {data}"),
                    Err(e) => error!("Failed to send: {e}"),
                }
            }
            Command::Sub(topic_name) => {
                let mut topic_guard = topics.lock().await;
                let topic = topic_guard
                    .entry(topic_name.to_owned())
                    .or_insert_with(|| Topic::new());

                let mut receiver = topic.sender.subscribe();
                let w = writer.clone();
                tokio::spawn(async move {
                    loop {
                        match receiver.recv().await {
                            Ok(x) => {
                                info!("message received: {x}");
                                let mut writer_guard = w.lock().await;
                                writer_guard.write_all("qqqqqq".as_bytes()).await.unwrap();
                            }
                            Err(e) => {
                                error!("Failed to receive: {e}");
                                continue;
                            }
                        }
                    }
                });
            }
        }

        Ok(())
    }
}

impl FromStr for Command {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let command = s.trim().split_whitespace().collect::<Vec<_>>();
        if command.len() < 2 {
            return Err(error::Error::InvalidCommand);
        }

        match command[0] {
            "PUB" => {
                if command.len() < 3 {
                    Err(error::Error::InvalidCommand)
                } else {
                    Ok(Command::Pub(command[1].to_owned(), command[2].to_owned()))
                }
            }
            "SUB" => Ok(Command::Sub(command[1].to_owned())),
            _ => Err(error::Error::InvalidCommand),
        }
    }
}
