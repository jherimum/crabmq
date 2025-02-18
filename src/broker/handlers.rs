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

pub async fn handle_publish_message(
    request: PublishMessageCommand,
    exchanges: &ExchangeMap,
) -> BrokerResult<()> {
    exchanges
        .get(&request.exchange_name)
        .unwrap()
        .lock()
        .await
        .publish(request.message)
        .await
}
