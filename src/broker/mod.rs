use commands::{
    BrokerCommand, BrokerResponse, IntoBrokerCommand, PublishMessageCommand,
};
use handlers::handle_ack_message;
use log::info;
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};
use crate::{
    exchange::{Exchange, ExchangeName},
    message::{Message, MessageId},
    queue::{Queue, QueueName},
};
use std::{collections::HashMap, sync::Arc};

pub mod commands;
pub mod handlers;

type QueueMap = HashMap<QueueName, Arc<Mutex<Queue>>>;
type ExchangeMap = HashMap<ExchangeName, Arc<Mutex<Exchange>>>;
type BrokerCommandSender = tokio::sync::mpsc::Sender<BrokerCommand>;

type AsyncCommandSender<T> =
    tokio::sync::oneshot::Sender<Result<BrokerResponse<T>, Error>>;

pub type BrokerResult<T> = Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error")]
    IoError(#[from] std::io::Error),

    #[error("Queue {0} does no exists")]
    QueueNotFound(QueueName),

    #[error("Exchange {0} does no exists")]
    ExchangeNotFound(ExchangeName),

    #[error("Failed to send message")]
    RecvError(#[from] oneshot::error::RecvError),

    #[error("Failed to send message")]
    SendError(#[from] mpsc::error::SendError<BrokerCommand>),
}

#[derive(Debug, Clone)]
pub struct BrokerCommandBus {
    sender: BrokerCommandSender,
}

impl BrokerCommandBus {
    pub async fn send<C>(
        &self,
        command: C,
    ) -> BrokerResult<BrokerResponse<C::Output>>
    where
        C: IntoBrokerCommand,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(command.into_command(tx)).await?;
        rx.await?
    }
}

pub struct Broker {
    queues: QueueMap,
    exchanges: ExchangeMap,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            queues: QueueMap::default(),
            exchanges: ExchangeMap::default(),
        }
    }

    async fn handle_publish_message(
        &self,
        request: PublishMessageCommand,
        tx: AsyncCommandSender<()>,
    ) {
        let response = self
            .exchanges
            .get(&request.exchange_name)
            .unwrap()
            .lock()
            .await
            .publish(request.message)
            .await;

        tx.send(response.map(|response| BrokerResponse { response }));
    }

    pub fn run(self) -> (JoinHandle<()>, BrokerCommandBus) {
        info!("Starting broker...");
        let (tx, mut rx) = mpsc::channel(100);

        let join = tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    BrokerCommand::PublishMesage(envelop) => {
                        self.handle_publish_message(
                            envelop.request,
                            envelop.sender,
                        )
                        .await;
                    }
                    BrokerCommand::AckMessage(envelop) => {
                        let response =
                            handle_ack_message(envelop.request, &self.queues)
                                .await;
                        let _ = envelop.sender.send(
                            response.map(|r| BrokerResponse { response: r }),
                        );
                    }
                }
            }
        });

        (join, BrokerCommandBus { sender: tx })
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
    use crate::message::{Message, MessageId};

    use super::Broker;

    #[tokio::test]
    async fn test() {
        let broker = Broker::new();

        broker
            .publish(
                &"name".try_into().unwrap(),
                Message::new(MessageId::new(), "Hello".as_bytes(), None),
            )
            .await
            .unwrap();
    }
}
