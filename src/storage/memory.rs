use std::collections::HashMap;
use crate::{
    exchange::{Exchange, ExchangeName},
    queue::{Queue, QueueName},
};

use super::{Storage, StorageError};

pub struct InMemryStorage;

impl Storage for InMemryStorage {
    fn load_queue(&self) -> StorageError<Vec<Queue>> {
        let q1 = Queue::new("q1".try_into().unwrap());
        let q2 = Queue::new("q2".try_into().unwrap());
        let q3 = Queue::new("q3".try_into().unwrap());
        let q4 = Queue::new("q4".try_into().unwrap());
        let q5 = Queue::new("q5".try_into().unwrap());

        Ok(vec![q1, q2, q3, q4, q5])
    }

    fn load_exchanges(&self) -> super::StorageError<Vec<Exchange>> {
        let e1 = Exchange::new("e1".try_into().unwrap());
        let e2 = Exchange::new("e2".try_into().unwrap());
        let e3 = Exchange::new("e3".try_into().unwrap());
        let e4 = Exchange::new("e4".try_into().unwrap());
        let e5 = Exchange::new("e5".try_into().unwrap());

        Ok(vec![e1, e2, e3, e4, e5])
    }

    fn load_bindings(
        &self,
    ) -> StorageError<HashMap<ExchangeName, Vec<QueueName>>> {
        Ok(Default::default())
    }
}
