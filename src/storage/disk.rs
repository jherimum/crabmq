use std::{
    io::{self},
    mem,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use tap::TapFallible;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

type MessageBytes = Vec<u8>;

const MAX_FILE_SIZE: u64 = 100; // 1MB

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Head {
    segment: u64,
    offset: usize,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct State {
    head: Head,
    segment: u64,
    base_path: PathBuf,
}

impl State {
    fn head(&self) -> &Head {
        &self.head
    }

    async fn set_offset(&mut self, offset: usize) -> Result<(), io::Error> {
        self.head.offset = offset;
        self.persist().await
    }

    async fn offset(&self) -> usize {
        self.head.offset
    }

    async fn increment_segment(&mut self) -> Result<(), io::Error> {
        self.segment += 1;
        self.persist().await
    }

    async fn persist(&self) -> Result<(), io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.state_file_name())
            .await?;

        file.write_all(&bincode::serialize(&self).unwrap()).await?;
        file.flush().await?;
        file.sync_all().await?;

        Ok(())
    }

    fn state_file_name(&self) -> PathBuf {
        self.base_path.join("state.dat")
    }
}

#[derive(Debug)]
pub struct QueueStorage {
    base_path: PathBuf,
    state: State,
}

impl QueueStorage {
    fn queue_file_name(&self, segment: u64) -> PathBuf {
        self.base_path.join(format!("queue_{}.dat", segment))
    }

    fn state_file_name(&self) -> PathBuf {
        self.base_path.join("state.dat")
    }

    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            state: State {
                base_path: base_path.as_ref().to_path_buf(),
                ..Default::default()
            },
        }
    }

    pub async fn read_segment_file(
        &self,
        segment: u64,
    ) -> std::io::Result<File> {
        OpenOptions::new()
            .read(true)
            .open(self.queue_file_name(segment))
            .await
    }

    pub fn generate_checksum(&self, data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    pub async fn open_current_segment(&mut self) -> std::io::Result<File> {
        log::trace!("Opening current segment: {}", self.state.segment);
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.queue_file_name(self.state.segment))
            .await
            .tap_err(|e| log::error!("Error opening file: {e}"))?;

        if file.metadata().await?.size() > MAX_FILE_SIZE {
            log::info!("Segment {} is full", self.state.segment);
            self.state
                .increment_segment()
                .await
                .tap_err(|e| log::error!("Failed to increment segment: {e}"))?;

            log::info!("Opening new segment: {}", self.state.segment);
            file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.queue_file_name(self.state.segment))
                .await
                .tap_err(|e| log::error!("Error opening file: {e}"))?;
        }

        Ok(file)
    }

    pub async fn push(&mut self, data: &[u8]) -> std::io::Result<()> {
        let file = self
            .open_current_segment()
            .await
            .tap_err(|e| log::error!("Failed do open current segment: {e}"))?;

        let mut buffer = BufWriter::new(file);
        buffer
            .write_all(&build_entry(data))
            .await
            .tap_err(|e| log::error!("Failed to write message: {e}"))?;

        buffer
            .flush()
            .await
            .tap_err(|e| log::error!("Failed to flush file: {e}"))?;

        buffer
            .get_ref()
            .sync_all()
            .await
            .tap_err(|e| log::error!("Failed to syn file: {e}"))?;

        Ok(())
    }

    async fn pop(&mut self, size: u64) -> std::io::Result<Vec<MessageBytes>> {
        let mut messages = Vec::new();

        let segment_file =
            self.read_segment_file(self.state.head.segment).await;

        match segment_file {
            Ok(file) => {
                let meta = file.metadata().await?;
                if meta.len() == 0 {
                    return Ok(messages);
                }

                if self.state.head.offset as u64 >= meta.len() {
                    return Ok(messages);
                }

                let mut buffer = BufReader::new(file);

                let mut pop_offset = buffer
                    .seek(io::SeekFrom::Current(self.state.head.offset as i64))
                    .await? as usize;

                while messages.len() < (size as usize) {
                    let (data, offset) = read_from_buffer(&mut buffer).await?;
                    messages.push(data);
                    pop_offset += offset;
                }

                self.state.set_offset(pop_offset).await?;
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(messages);
                }
                return Err(e);
            }
        }

        dbg!(&self.state);

        Ok(messages)
    }
}

fn build_entry(data: &[u8]) -> Vec<u8> {
    let size = size_of::<u32>() + size_of::<usize>() + data.len();
    let mut bytes = Vec::with_capacity(size);
    let checksum = generate_checksum(data).to_le_bytes();

    bytes.extend_from_slice(checksum.as_ref());
    bytes.extend_from_slice(data.len().to_le_bytes().as_ref());
    bytes.extend_from_slice(data);

    bytes
}

pub fn generate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

async fn read_from_buffer(
    buffer: &mut BufReader<File>,
) -> io::Result<(Vec<u8>, usize)> {
    let mut checksum_buffer = [0; mem::size_of::<u32>()];
    buffer.read_exact(&mut checksum_buffer).await?;

    let checksum = u32::from_le_bytes(checksum_buffer);

    let mut size_buffer = [0; mem::size_of::<u64>()];
    buffer.read_exact(&mut size_buffer).await?;

    let size = u64::from_le_bytes(size_buffer);
    let mut data = vec![0; size as usize];
    buffer.read_exact(&mut data).await?;

    Ok((
        data,
        (mem::size_of::<u32>() + mem::size_of::<usize>() + size as usize),
    ))
}

#[cfg(test)]
mod test {

    use tokio::fs::remove_file;

    use super::*;

    #[tokio::test]
    async fn test_push() {
        dotenvy::dotenv().unwrap();
        env_logger::init();

        let path = "test-queue";

        if !PathBuf::from(path).exists() {
            tokio::fs::create_dir_all(path).await.unwrap();
        }

        let mut dir = tokio::fs::read_dir(path).await.unwrap();
        while let Some(entry) = dir.next_entry().await.unwrap() {
            let path = entry.path();
            remove_file(path).await.unwrap();
        }

        tokio::fs::create_dir_all(path).await.unwrap();

        let mut storage = QueueStorage::new(path);
        dbg!(&storage);

        storage.push(b"1000000000").await.unwrap();
        storage.push(b"2000000000").await.unwrap();
        storage.push(b"3000000000").await.unwrap();
        storage.push(b"4000000000").await.unwrap();
        storage.push(b"1000000000").await.unwrap();
        storage.push(b"2000000000").await.unwrap();
        storage.push(b"3000000000").await.unwrap();
        storage.push(b"4000000000").await.unwrap();
        storage.push(b"1000000000").await.unwrap();
        storage.push(b"2000000000").await.unwrap();
        storage.push(b"3000000000").await.unwrap();
        storage.push(b"4000000000").await.unwrap();
        storage.push(b"1000000000").await.unwrap();
        storage.push(b"2000000000").await.unwrap();
        storage.push(b"3000000000").await.unwrap();
        storage.push(b"4000000000").await.unwrap();

        dbg!(&storage);

        // let pop = storage.pop(1).await.unwrap();
        // assert_eq!(pop, vec![b"1".to_vec()]);

        // let pop = storage.pop(1).await.unwrap();
        // assert_eq!(pop, vec![b"2".to_vec()]);

        // let pop = storage.pop(2).await.unwrap();
        // assert_eq!(pop, vec![b"3".to_vec(), b"4".to_vec()]);
    }
}
