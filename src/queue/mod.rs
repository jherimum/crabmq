use crate::{
    broker::Error,
    message::{Message, MessageId},
};
use chrono::Utc;
use log::info;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Mutex, time::sleep};

pub struct QueueOptions {}

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

type MessageQueue = Arc<Mutex<VecDeque<Message>>>;
type PendingAcks = HashMap<MessageId, Message>;
type DelayedMessages = Arc<Mutex<BinaryHeap<DelayedMessage>>>;

#[derive(Clone, Debug)]
pub struct Queue {
    name: QueueName,
    messages: MessageQueue,
    pending_acks: PendingAcks,
    delayed_messages: DelayedMessages,
}

impl Queue {
    pub fn name(&self) -> &QueueName {
        &self.name
    }

    pub fn new(name: QueueName) -> Self {
        Self {
            name,
            messages: MessageQueue::default(),
            pending_acks: PendingAcks::default(),
            delayed_messages: DelayedMessages::default(),
        }
    }

    pub fn start(&self) {
        let messages = self.messages.clone();
        let delayed_messages = self.delayed_messages.clone();
        tokio::spawn(async move {
            process_delayed_messages(messages, delayed_messages).await;
        });
    }

    pub async fn publish(&mut self, message: Message) {
        self.messages.lock().await.push_back(message);
    }

    pub async fn publish_all(&mut self, mut messages: VecDeque<Message>) {
        if messages.is_empty() {
            return;
        }
        self.messages.lock().await.append(&mut messages);
    }

    pub async fn publish_many(&mut self, message: Vec<Message>) {
        let mut guard = self.messages.lock().await;
        for m in message {
            guard.push_back(m);
        }
    }

    pub async fn consume(&mut self, fetch_size: u64) -> Vec<Message> {
        let mut messages = Vec::new();
        while let Some(m) = self.messages.lock().await.pop_front() {
            messages.push(m.clone());

            self.pending_acks.insert(m.id().clone(), m);

            if messages.len() == fetch_size as usize {
                break;
            }
        }
        messages
    }

    pub async fn ack(&mut self, message_id: &MessageId) {
        self.pending_acks.remove(message_id);
    }

    pub async fn nack(&mut self, message_id: &MessageId) {
        if let Some(mut m) = self.pending_acks.remove(message_id) {
            if let Some(r) = m.retry() {
                self.delayed_messages.lock().await.push(DelayedMessage {
                    time: Reverse(
                        (Utc::now() + Duration::from_secs(r)).timestamp(),
                    ),
                    message: m,
                });
            }
        }
    }
}

async fn process_delayed_messages(
    messages: Arc<Mutex<VecDeque<Message>>>,
    delayed_messages: Arc<Mutex<BinaryHeap<DelayedMessage>>>,
) {
    loop {
        info!("Checking delayed messages");
        while let Some(delayed_message) = delayed_messages.lock().await.pop() {
            if Utc::now().timestamp() > delayed_message.time.0 {
                messages.lock().await.push_back(delayed_message.message);
            } else {
                break;
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Clone, Debug)]
struct DelayedMessage {
    time: Reverse<i64>,
    message: Message,
}

impl PartialEq for DelayedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

impl Eq for DelayedMessage {}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    #[test]
    fn name() {
        let now = chrono::Utc::now();
        let x = now.timestamp();
        println!("{}", now);

        let x = DateTime::from_timestamp(x, 0);

        println!("{:?}", x);
    }
}
