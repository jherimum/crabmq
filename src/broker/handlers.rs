use super::{
    commands::{AckMessageCommand, PublishMessageCommand},
    BrokerResult, ExchangeMap, QueueMap,
};

pub async fn handle_ack_message(
    request: AckMessageCommand,
    queues: &QueueMap,
) -> BrokerResult<()> {
    todo!()
}
