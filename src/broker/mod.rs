use commands::{
    AckMessageCommand, BrokerCommand, BrokerResponse, IntoBrokerCommand,
    NackMessageCommand, PublishMessageCommand,
};
use log::info;
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};
use crate::{
    exchange::{Exchange, ExchangeName},
    message::Message,
    queue::{Queue, QueueName},
    storage::{self, Storage},
};
use std::{collections::HashMap, sync::Arc};

pub mod commands;

type QueueMap = HashMap<QueueName, Arc<Mutex<Queue>>>;
type ExchangeMap = HashMap<ExchangeName, Arc<Mutex<Exchange>>>;
type BrokerCommandSender = mpsc::Sender<BrokerCommand>;
type AsyncCommandSender<T> = oneshot::Sender<Result<BrokerResponse<T>, Error>>;
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

    #[error("Storage error: {0}")]
    StorageError(#[from] storage::Error),
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
    storage: Arc<dyn Storage + Send + Sync>,
}

impl Broker {
    pub fn new(storage: Arc<dyn Storage + Send + Sync>) -> Self {
        Broker {
            queues: QueueMap::default(),
            exchanges: ExchangeMap::default(),
            storage,
        }
    }

    pub async fn load(&mut self) -> BrokerResult<()> {
        let queues = self.storage.load_queue()?;
        let exchanges = self.storage.load_exchanges()?;
        let bindings = self.storage.load_bindings()?;

        queues.into_iter().for_each(|queue| {
            self.queues
                .insert(queue.name().clone(), Arc::new(Mutex::new(queue)));
        });

        exchanges.into_iter().for_each(|exchange| {
            self.exchanges.insert(
                exchange.name().clone(),
                Arc::new(Mutex::new(exchange)),
            );
        });

        for (exchange, queues) in bindings {
            for queue in queues {
                let _ = self.bind_queue(&exchange, &queue).await;
            }
        }

        Ok(())
    }

    pub async fn run(
        mut self,
    ) -> BrokerResult<(JoinHandle<()>, BrokerCommandBus)> {
        info!("Starting broker...");

        self.load().await?;

        for exchange in self.exchanges.values() {
            exchange.lock().await.start();
        }

        for queue in self.queues.values() {
            queue.lock().await.start();
        }

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
                        self.handle_ack(envelop.request, envelop.sender).await;
                    }

                    BrokerCommand::NackMessage(envelop) => {
                        self.handle_nack(envelop.request, envelop.sender).await;
                    }
                }
            }
        });

        Ok((join, BrokerCommandBus { sender: tx }))
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

    async fn handle_ack(
        &self,
        request: AckMessageCommand,
        tx: AsyncCommandSender<()>,
    ) {
        self.queues
            .get(&request.queue_name)
            .unwrap()
            .lock()
            .await
            .ack(&request.message_id)
            .await;

        tx.send(Ok(BrokerResponse { response: () }));
    }

    async fn handle_nack(
        &self,
        request: NackMessageCommand,
        tx: AsyncCommandSender<()>,
    ) {
        self.queues
            .get(&request.queue_name)
            .unwrap()
            .lock()
            .await
            .nack(&request.message_id)
            .await;

        tx.send(Ok(BrokerResponse { response: () }));
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        message::{Message, MessageId},
        storage::memory::InMemryStorage,
    };

    use super::Broker;

    #[tokio::test]
    async fn test() {
        let broker = Broker::new(Arc::new(InMemryStorage));
    }
}
