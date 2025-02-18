use crate::broker::Error;
use crate::message::Message;
use crate::queue::{Queue, QueueName};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use wal::ExhangeWAL;

pub mod wal;

type Bindings = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub struct ExchangeOptions {}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ExchangeName(String);

impl TryFrom<&str> for ExchangeName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(ExchangeName(value.to_owned()))
    }
}

impl Display for ExchangeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug)]
pub struct Exchange {
    name: ExchangeName,
    bindings: Bindings,
    wal: ExhangeWAL,
}

impl Exchange {
    pub async fn new(name: ExchangeName, wal: ExhangeWAL) -> Self {
        let bindings: Bindings = Default::default();

        //publish_to_bindings(bindings.clone(), wal.clone());

        Self {
            name: name.clone(),
            bindings,
            wal,
        }
    }

    pub fn bind_queue(&mut self, name: QueueName, queue: Arc<Mutex<Queue>>) {
        self.bindings.insert(name, queue);
    }

    pub fn unbind_queue(&mut self, queue_name: &QueueName) {
        self.bindings.remove(queue_name);
    }

    pub fn bindings(&self) -> Vec<&Arc<Mutex<Queue>>> {
        self.bindings.values().collect()
    }

    pub async fn publish(&mut self, message: Message) -> Result<(), Error> {
        //self.wal.lock().await.write(&message).await.unwrap();
        Ok(())
    }
}

// fn publish_to_bindings(bindings: Bindings, wal: Arc<Mutex<ExhangeWAL>>) {
//     tokio::spawn(async move {
//         loop {
//             let messages = wal.lock().await.read().await.unwrap();
//             for queue in bindings.values() {
//                 let mut queue = queue.lock().await;
//                 queue.publish_many(messages.clone()).await;
//             }

//             sleep(Duration::from_secs(1)).await;
//         }
//     });
// }
