use tokio::sync::oneshot::Sender;
use crate::{
    exchange::ExchangeName,
    message::{Message, MessageId},
    queue::QueueName,
};
use super::{BrokerResult, Error};

pub enum BrokerCommand {
    PublishMesage(Command<PublishMessageCommand, ()>),
    AckMessage(Command<AckMessageCommand, ()>),
}

pub struct Command<C, O> {
    pub request: C,
    pub sender: Sender<Result<BrokerResponse<O>, Error>>,
}

pub trait IntoBrokerCommand {
    type Output;
    fn into_command(
        self,
        tx: Sender<BrokerResult<BrokerResponse<Self::Output>>>,
    ) -> BrokerCommand;
}

pub struct PublishMessageCommand {
    pub exchange_name: ExchangeName,
    pub message: Message,
}

impl IntoBrokerCommand for PublishMessageCommand {
    type Output = ();
    fn into_command(
        self,
        tx: Sender<BrokerResult<BrokerResponse<Self::Output>>>,
    ) -> BrokerCommand {
        BrokerCommand::PublishMesage(Command {
            request: self,
            sender: tx,
        })
    }
}

pub struct AckMessageCommand {
    pub queue_name: QueueName,
    pub message_id: MessageId,
}

impl IntoBrokerCommand for AckMessageCommand {
    type Output = ();
    fn into_command(
        self,
        tx: Sender<BrokerResult<BrokerResponse<Self::Output>>>,
    ) -> BrokerCommand {
        BrokerCommand::AckMessage(Command {
            request: self,
            sender: tx,
        })
    }
}

pub struct BrokerResponse<R> {
    pub response: R,
}
