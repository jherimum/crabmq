use tokio::sync::Mutex;

use crate::{
    exchange::{Exchange, ExchangeName},
    message::{Message, MessageId},
    queue::{Queue, QueueName},
};
use std::{collections::HashMap, sync::Arc};

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

pub struct Broker {
    queues: HashMap<QueueName, Arc<Mutex<Queue>>>,
    exchanges: HashMap<ExchangeName, Arc<Mutex<Exchange>>>,
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

    pub fn declare_queue(&mut self, name: QueueName) {
        self.queues
            .entry(name.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Queue::new(name))));
    }

    pub fn declare_exchange(&mut self, name: ExchangeName) {
        // self.exchanges.insert(
        //     name.clone(),
        //     Arc::new(Mutex::new(Exchange::new(name, exchange_type))),
        // );
        todo!()
    }

    pub async fn bind_queue(
        &mut self,
        exchange_name: &ExchangeName,
        queue_name: &QueueName,
    ) -> BrokerResult<()> {
        match (
            self.exchanges.get_mut(exchange_name),
            self.queues.get(&queue_name),
        ) {
            (Some(ex), Some(q)) => {
                let mut ex_guard = ex.lock().await;
                ex_guard.bind_queue(queue_name.clone(), Arc::clone(q));
                Ok(())
            }
            (None, _) => Err(Error::ExchangeNotFound(exchange_name.clone())),
            (_, None) => Err(Error::QueueNotFound(queue_name.to_owned())),
        }
    }

    pub async fn publish(
        &self,
        exchange_name: &ExchangeName,
        message: Message,
    ) -> BrokerResult<()> {
        if let Some(ex) = self.exchanges.get(exchange_name) {
            let mut ex_guard = ex.lock().await;
            ex_guard.publish(message.clone()).await
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
            Some(queue) => {
                let mut queue_guard = queue.lock().await;
                Ok(queue_guard.consume(fetch_size).await)
            }
            None => Err(Error::QueueNotFound(queue_name.to_owned())),
        }
    }

    pub async fn ack(&self, queue_name: &QueueName, message_id: &MessageId) {
        if let Some(q) = self.queues.get(queue_name) {
            let mut q_guard = q.lock().await;
            q_guard.ack(message_id).await;
        }
    }

    pub async fn nack(&self, queue_name: &QueueName, message_id: &MessageId) {
        if let Some(q) = self.queues.get(queue_name) {
            let mut q_guard = q.lock().await;
            q_guard.nack(message_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{rngs::StdRng, Rng, SeedableRng};
    use tokio::time::sleep;

    use crate::{
        message::{self, Message, MessageId},
    };

    use super::Broker;

    #[tokio::test]
    async fn test_broker() {
        dotenvy::dotenv().unwrap();
        env_logger::init();

        let mut broker = Broker::new();
        broker.declare_queue("fila1".try_into().unwrap());
        broker.declare_exchange("exchange1".try_into().unwrap());
        broker
            .bind_queue(
                &"exchange1".try_into().unwrap(),
                &"fila1".try_into().unwrap(),
            )
            .await;

        sleep(Duration::from_secs(10)).await
    }
}
