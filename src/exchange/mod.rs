use crate::broker::Error;
use crate::message::Message;
use crate::queue::{Queue, QueueName};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

type Bindings = HashMap<QueueName, Arc<Mutex<Queue>>>;
type Messages = Arc<Mutex<VecDeque<Message>>>;

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
    messages: Messages,
}

impl Exchange {
    pub fn new(name: ExchangeName) -> Self {
        let bindings: Bindings = Bindings::default();
        let messages = Messages::default();

        Self {
            name: name.clone(),
            bindings,
            messages,
        }
    }

    pub fn start(&self) {
        publish_to_bindings(self.bindings.clone(), self.messages.clone());
    }

    pub fn name(&self) -> &ExchangeName {
        &self.name
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
        //persist to WAL
        self.messages.lock().await.push_back(message);
        Ok(())
    }
}

fn publish_to_bindings(bindings: Bindings, messages: Messages) {
    tokio::spawn(async move {
        loop {
            let mut messages = messages.lock().await;

            if !messages.is_empty() {
                for queue in bindings.values() {
                    let mut queue = queue.lock().await;
                    queue.publish_all(messages.clone()).await;
                }
            }
            messages.clear();

            sleep(Duration::from_secs(1)).await;
        }
    });
}
