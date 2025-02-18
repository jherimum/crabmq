use crate::{
    broker::Error,
    message::{Message, MessageId},
};
use log::info;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::Mutex,
    time::{sleep, Instant},
};

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

#[derive(Clone, Debug)]
pub struct Queue {
    name: QueueName,
    messages: Arc<Mutex<VecDeque<Message>>>,
    pending_acks: HashMap<MessageId, Message>,
    delayed_messages: Arc<Mutex<BinaryHeap<DelayedMessage>>>,
}

impl Queue {
    pub fn name(&self) -> &QueueName {
        &self.name
    }

    pub fn new(name: QueueName) -> Self {
        let pending_acks = Default::default();
        let delayed_messages = Default::default();

        let queue = Self {
            name,
            messages: Default::default(),
            pending_acks,
            delayed_messages,
        };

        tokio::spawn(process_delayed_messages(
            queue.messages.clone(),
            queue.delayed_messages.clone(),
        ));

        queue
    }

    pub async fn publish(&mut self, message: Message) {
        self.messages.lock().await.push_back(message);
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
                    time: Reverse(Instant::now() + Duration::from_secs(r)),
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
            if Instant::now() > delayed_message.time.0 {
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
    time: Reverse<Instant>,
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
