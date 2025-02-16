use log::warn;

use crate::{
    exchange::{Exchange, ExchangeName, ExchangeType},
    message::{Message, MessageId},
    queue::{Queue, QueueName},
};
use std::collections::HashMap;

pub type BrokerResult<T> = Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error")]
    IoError(#[from] std::io::Error),

    #[error("Queue {0} does no exists")]
    QueueNotFound(QueueName),

    #[error("Exchange {0} does no exists")]
    ExchangeNotFound(ExchangeName),
}

#[derive(Clone)]
pub struct Broker {
    queues: HashMap<QueueName, Queue>,
    exchanges: HashMap<ExchangeName, Exchange>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            queues: Default::default(),
            exchanges: Default::default(),
        }
    }

    pub async fn start(&self) -> BrokerResult<()> {
        Ok(())
    }

    pub async fn declare_queue(&mut self, name: QueueName) {
        self.queues
            .entry(name.clone())
            .or_insert_with(|| Queue::new(name));
    }

    pub fn declare_exchange(&mut self, name: ExchangeName, exchange_type: ExchangeType) {
        self.exchanges
            .insert(name.clone(), Exchange::new(name, exchange_type));
    }

    pub fn bind_queue(
        &mut self,
        exchange_name: &ExchangeName,
        queue_name: QueueName,
    ) -> BrokerResult<()> {
        match self.exchanges.get_mut(exchange_name) {
            Some(ex) => match self.queues.get(&queue_name) {
                Some(_) => {
                    ex.bind_queue(queue_name);
                    Ok(())
                }
                None => Err(Error::QueueNotFound(queue_name.to_owned())),
            },
            None => Err(Error::ExchangeNotFound(exchange_name.clone())),
        }
    }

    pub async fn publish(
        &self,
        exchange_name: &ExchangeName,
        message: Message,
    ) -> BrokerResult<()> {
        if let Some(ex) = self.exchanges.get(exchange_name) {
            for queue in ex.bindings() {
                if let Some(queue) = self.queues.get(queue) {
                    queue.publish(message.clone()).await;
                } else {
                    warn!("Queue not found");
                }
            }
            Ok(())
        } else {
            Err(Error::ExchangeNotFound(exchange_name.to_owned()))
        }
    }

    pub async fn consume_from_queue(
        &self,
        queue_name: &QueueName,
        fetch_size: u64,
    ) -> BrokerResult<Vec<Message>> {
        match self.queues.get(queue_name) {
            Some(queue) => Ok(queue.consume(fetch_size).await),
            None => Err(Error::QueueNotFound(queue_name.to_owned())),
        }
    }

    pub async fn ack(&self, queue_name: &QueueName, message_id: &MessageId) {
        if let Some(q) = self.queues.get(queue_name) {
            q.ack(message_id).await;
        }
    }

    pub async fn nack(&self, queue_name: &QueueName, message_id: &MessageId) {
        if let Some(q) = self.queues.get(queue_name) {
            q.nack(message_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{rngs::StdRng, Rng, SeedableRng};
    use tokio::time::sleep;

    use crate::message::{self, Message, MessageId};

    use super::Broker;

    #[tokio::test]
    async fn test_broker() {
        let mut broker = Broker::new();

        broker.declare_queue("fila1".try_into().unwrap()).await;

        let prod = broker.clone();
        tokio::spawn(async move {
            for i in 0..1000 {
                // let mut rng = {
                //     let mut rng = rand::rng();
                //     StdRng::from_rng(&mut rng)
                // };
                // let millis = rng.gen_range(1..100);
                tokio::time::sleep(std::time::Duration::from_millis(3)).await;
                println!("publish message {i}");
                prod.publish(
                    &"fila1".try_into().unwrap(),
                    Message::new(MessageId::new(), ""),
                )
                .await
                .unwrap();
            }
        });

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let messages = broker
                    .consume_from_queue(&"fila1".try_into().unwrap(), 100)
                    .await
                    .unwrap();
                println!("Consuming {}", messages.len());
            }
        })
        .await
        .unwrap();
    }
}
