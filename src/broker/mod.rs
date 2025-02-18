use commands::{BrokerCommand, BrokerResponse, IntoBrokerCommand};
use handlers::{handle_ack_message, handle_publish_message};
use tokio::sync::Mutex;
use crate::{
    exchange::{Exchange, ExchangeName},
    message::{Message, MessageId},
    queue::{Queue, QueueName},
};
use std::{collections::HashMap, future::Future, sync::Arc};

pub mod commands;
pub mod handlers;

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
    RecvError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("Failed to send message")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<BrokerCommand>),
}

type QueueMap = HashMap<QueueName, Arc<Mutex<Queue>>>;
type ExchangeMap = HashMap<ExchangeName, Arc<Mutex<Exchange>>>;
type BrokerCommandSender = tokio::sync::mpsc::Sender<BrokerCommand>;
type BrokerCommandReceiver = tokio::sync::mpsc::Receiver<BrokerCommand>;

pub struct Broker {
    queues: QueueMap,
    exchanges: ExchangeMap,
    sender: BrokerCommandSender,
}

impl Broker {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let queues = QueueMap::default();
        let exchanges = ExchangeMap::default();

        spawn_command_executor(rx, exchanges.clone(), queues.clone());

        Broker {
            queues,
            exchanges,
            sender: tx,
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

pub trait BrokerCommandBus {
    fn send<C>(
        &self,
        command: C,
    ) -> impl Future<Output = BrokerResult<BrokerResponse<C::Output>>>
    where
        C: IntoBrokerCommand;
}

impl BrokerCommandBus for &Broker {
    async fn send<C>(
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

fn spawn_command_executor(
    mut rx: BrokerCommandReceiver,
    exchanges: ExchangeMap,
    queues: QueueMap,
) {
    tokio::spawn(async move {
        while let Some(command) = rx.recv().await {
            match command {
                BrokerCommand::PublishMesage(envelop) => {
                    let response =
                        handle_publish_message(envelop.request, &exchanges)
                            .await;
                    let _ = envelop
                        .sender
                        .send(response.map(|r| BrokerResponse { response: r }));
                }
                BrokerCommand::AckMessage(envelop) => {
                    let response =
                        handle_ack_message(envelop.request, &queues).await;
                    let _ = envelop
                        .sender
                        .send(response.map(|r| BrokerResponse { response: r }));
                }
            }
        }
    });
}
