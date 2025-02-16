use crate::broker::Error;
use crate::queue::QueueName;
use std::collections::HashSet;
use std::fmt::Display;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ExchangeName(String);

impl TryFrom<&str> for ExchangeName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(ExchangeName(value.to_owned()))
    }
}

impl Display for ExchangeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone)]
pub struct Exchange {
    name: ExchangeName,
    exchange_type: ExchangeType,
    bindings: HashSet<QueueName>,
}

impl Exchange {
    pub fn new(name: ExchangeName, exchange_type: ExchangeType) -> Self {
        Self {
            name,
            exchange_type,
            bindings: Default::default(),
        }
    }

    pub fn bind_queue(&mut self, queue_name: QueueName) {
        self.bindings.insert(queue_name);
    }

    pub fn unbind_queue(&mut self, queue_name: &QueueName) {
        self.bindings.remove(queue_name);
    }

    pub fn bindings(&self) -> &HashSet<QueueName> {
        &self.bindings
    }
}

#[derive(Debug, Clone)]
pub enum ExchangeType {
    Fanout,
    Direct,
    Topic,
}
