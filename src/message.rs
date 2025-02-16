#[derive(Debug, Clone)]
pub struct Message {
    id: MessageId,
    data: Vec<u8>,
}

impl Message {
    pub fn new<D: AsRef<[u8]>>(id: MessageId, data: D) -> Self {
        Message {
            id,
            data: Vec::from(data.as_ref()),
        }
    }

    pub fn id(&self) -> &MessageId {
        &self.id
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MessageId(uuid::Uuid);

impl MessageId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
