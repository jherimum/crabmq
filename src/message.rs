use borsh_derive::{BorshDeserialize, BorshSerialize};
use serde::Serialize;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct Message {
    id: MessageId,
    data: Vec<u8>,
    retry_policy: Option<RetryPolicy>,
    retries: u8,
}

impl Message {
    pub fn new<D: AsRef<[u8]>>(
        id: MessageId,
        data: D,
        retry_policy: Option<RetryPolicy>,
    ) -> Self {
        Message {
            id,
            data: Vec::from(data.as_ref()),
            retry_policy,
            retries: 0,
        }
    }

    pub fn id(&self) -> &MessageId {
        &self.id
    }

    pub fn retry(&mut self) -> Option<u64> {
        self.retries += 1;
        self.retry_policy
            .as_ref()
            .and_then(|rp| rp.retry(self.retries))
    }
}

#[derive(
    Debug, Clone, Hash, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
pub struct MessageId(String);

impl MessageId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, Serialize, BorshSerialize, BorshDeserialize)]
pub enum RetryPolicy {
    Fixed { delay: u64, max: u8 },
    Unlimited { delay: u64 },
}

impl RetryPolicy {
    pub fn retry(&self, actual_retries: u8) -> Option<u64> {
        match self {
            RetryPolicy::Fixed { delay, max } => {
                if actual_retries < *max {
                    Some(*delay)
                } else {
                    None
                }
            }
            RetryPolicy::Unlimited { delay } => Some(*delay),
        }
    }
}
