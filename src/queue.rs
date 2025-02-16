use crate::{
    broker::Error,
    message::{Message, MessageId},
};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct QueueName(String);

impl Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for QueueName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(QueueName(value.to_owned()))
    }
}

#[derive(Clone)]
pub struct Queue {
    name: QueueName,
    messages: Arc<Mutex<VecDeque<Message>>>,
    pending_acks: Arc<Mutex<HashMap<MessageId, Message>>>,
}

impl Queue {
    pub fn new(name: QueueName) -> Self {
        Self {
            name,
            messages: Default::default(),
            pending_acks: Default::default(),
        }
    }

    pub async fn publish(&self, message: Message) {
        let mut guard = self.messages.lock().await;
        guard.push_back(message);
    }

    pub async fn consume(&self, fetch_size: u64) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut guard = self.messages.lock().await;
        while let Some(m) = guard.pop_front() {
            messages.push(m.clone());
            let mut acks_guard = self.pending_acks.lock().await;
            acks_guard.insert(m.id().clone(), m);

            if messages.len() == fetch_size as usize {
                break;
            }
        }
        messages
    }

    pub async fn ack(&self, message_id: &MessageId) {
        let mut guard = self.pending_acks.lock().await;
        guard.remove(message_id);
    }

    pub async fn nack(&self, message_id: &MessageId) {
        let mut guard = self.pending_acks.lock().await;
        if let Some(m) = guard.remove(message_id) {
            let mut guard = self.messages.lock().await;
            guard.push_front(m);
        }
    }
}
