use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use chrono::Utc;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};
use crate::broker::Error;

const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024; // 10MB per log file

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry {
    index: u64,
    timestamp: u64,
    checksum: u32,
    data: Vec<u8>,
}

impl LogEntry {
    fn validate(&self) -> bool {
        checksum(&self.data) == self.checksum
    }
}

#[derive(Debug, Clone)]
pub struct ExhangeWAL {
    dir: PathBuf,
    current_segment: u64,
    file: Arc<Mutex<BufWriter<File>>>,
    file_size: u64,
}

fn checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

impl ExhangeWAL {
    async fn new(log_dir: &str) -> Result<Self, Error> {
        let dir = PathBuf::from(log_dir);
        tokio::fs::create_dir_all(&dir).await?;

        let current_segment = Self::load_metadata(&dir).await;
        let (file, file_size) =
            Self::open_log_segment(&dir, current_segment).await?;

        Ok(Self {
            dir,
            current_segment,
            file: Arc::new(Mutex::new(BufWriter::new(file))),
            file_size,
        })
    }

    async fn append(&mut self, data: Vec<u8>) -> std::io::Result<()> {
        let entry = LogEntry {
            index: self.current_segment,
            timestamp: Utc::now().timestamp() as u64,
            checksum: checksum(&data),
            data,
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let entry_size = (encoded.len() as u64) + 8;

        // Rotate if needed
        if self.file_size + entry_size > MAX_LOG_SIZE {
            self.rotate_log().await?;
        }

        let mut guard = self.file.lock().await;
        guard
            .write_all(&(encoded.len() as u64).to_le_bytes())
            .await?;
        guard.write_all(&encoded).await?;
        guard.flush().await?;
        guard.get_ref().sync_all().await?;
        self.file_size += entry_size;

        Ok(())
    }

    async fn rotate_log(&mut self) -> std::io::Result<()> {
        self.current_segment += 1;
        Self::save_metadata(&self.dir, self.current_segment);
        let (new_file, new_size) =
            Self::open_log_segment(&self.dir, self.current_segment).await?;

        self.file = Arc::new(Mutex::new(BufWriter::new(new_file)));
        self.file_size = new_size;
        Ok(())
    }

    async fn save_metadata(dir: &Path, segment: u64) -> Result<(), Error> {
        let meta_path = dir.join("wal.meta");
        let _ = tokio::fs::write(meta_path, segment.to_string()).await?;
        Ok(())
    }

    async fn open_log_segment(
        dir: &Path,
        segment: u64,
    ) -> std::io::Result<(File, u64)> {
        let path = dir.join(format!("wal_{}.log", segment));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let file_size = file.metadata().await?.len();
        Ok((file, file_size))
    }

    async fn load_metadata(dir: &Path) -> u64 {
        let meta_path = dir.join("wal.meta");
        if let Ok(contents) = tokio::fs::read_to_string(&meta_path).await {
            contents.parse().unwrap_or(0)
        } else {
            0
        }
    }

    // pub async fn write(&mut self, message: &Message) -> Result<(), Error> {
    //     let ser = to_vec(message).unwrap();
    //     let mut writer = BufWriter::new(&mut self.file);
    //     writer.write_all(&ser.len().to_le_bytes()).await.unwrap();
    //     writer.write_all(&ser).await.unwrap();
    //     writer.flush().await.unwrap();
    //     Ok(())
    // }

    // pub async fn read(&mut self) -> Result<Vec<Message>, Error> {
    //     let mut reader = BufReader::new(&mut self.file);
    //     let mut messages = Vec::new();

    //     loop {
    //         let mut buf = Vec::new();
    //         reader.read_to_end(&mut buf).await.unwrap();

    //         println!("{:?}", buf);

    //         //println!("{:?}", buf);

    //         break;

    //         // let mut size_bytes = [0u8; std::mem::size_of::<usize>()];
    //         // match reader.read_exact(&mut size_bytes).await {
    //         //     Ok(x) => {
    //         //         let size = usize::from_le_bytes(size_bytes) as usize;
    //         //         let mut buffer = vec![0u8; size];
    //         //         reader.read_exact(&mut buffer).await?;
    //         //         messages.push(borsh::from_slice(&buffer).unwrap());
    //         //     }
    //         //     Err(e) => {
    //         //         println!("EOF: {} ", e);
    //         //         break;
    //         //     }
    //         // }
    //     }

    //     Ok(messages)
    // }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn write_to_file() {}
}
