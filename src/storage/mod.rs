use std::collections::HashMap;

use crate::{
    exchange::{Exchange, ExchangeName},
    queue::{Queue, QueueName},
};

pub mod disk;
pub mod memory;

pub type StorageError<T> = Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {}

pub trait Storage {
    fn load_queue(&self) -> StorageError<Vec<Queue>>;

    fn load_exchanges(&self) -> StorageError<Vec<Exchange>>;

    fn load_bindings(
        &self,
    ) -> StorageError<HashMap<ExchangeName, Vec<QueueName>>>;
}
