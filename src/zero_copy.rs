use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

/// -----------------------------
/// Message batch with indexed access
/// -----------------------------
#[derive(Debug, Clone)]
pub struct MessageBatch {
    /// Index of message offsets (u32 LE)
    pub indexes: Bytes,
    /// Raw concatenated message payloads
    pub messages: Bytes,
    /// Number of messages
    pub count: usize,
}

impl MessageBatch {
    /// Create a batch from raw message slices
    pub fn from_messages(messages: &[&[u8]]) -> Self {
        let count = messages.len();

        let mut index_buf = BytesMut::with_capacity(count * 4);
        let total_size: usize = messages.iter().map(|m| m.len()).sum();
        let mut msg_buf = BytesMut::with_capacity(total_size);

        let mut offset = 0u32;
        for msg in messages {
            index_buf.put_u32_le(offset);
            msg_buf.put_slice(msg);
            offset += msg.len() as u32;
        }

        Self {
            indexes: index_buf.freeze(),
            messages: msg_buf.freeze(),
            count,
        }
    }

    /// Zero-copy iterator
    pub fn iter(&self) -> MessageBatchIterator {
        MessageBatchIterator {
            indexes: self.indexes.clone(),
            messages: self.messages.clone(),
            current: 0,
            count: self.count,
        }
    }

    /// Get message by index (zero-copy)
    pub fn get(&self, index: usize) -> Option<Bytes> {
        if index >= self.count {
            return None;
        }

        let mut idx = self.indexes.clone();
        idx.advance(index * 4);
        let start = idx.get_u32_le() as usize;

        let end = if index + 1 < self.count {
            idx.get_u32_le() as usize
        } else {
            self.messages.len()
        };

        Some(self.messages.slice(start..end))
    }

    pub fn total_size(&self) -> usize {
        self.indexes.len() + self.messages.len()
    }
}

/// -----------------------------
/// Iterator over MessageBatch
/// -----------------------------
pub struct MessageBatchIterator {
    indexes: Bytes,
    messages: Bytes,
    current: usize,
    count: usize,
}

impl Iterator for MessageBatchIterator {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            return None;
        }

        let start = self.indexes.get_u32_le() as usize;

        let end = if self.current + 1 < self.count {
            let mut peek = self.indexes.clone();
            peek.get_u32_le() as usize
        } else {
            self.messages.len()
        };

        self.current += 1;
        Some(self.messages.slice(start..end))
    }
}

/// -----------------------------
/// SMS Message View
/// -----------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMSMessageView {
    pub from: String,
    pub to: String,
    pub body: String,
    pub timestamp: i64,
    pub conversation_id: String,
}

impl SMSMessageView {
    pub fn to_bytes(&self) -> Result<Bytes> {
        Ok(Bytes::from(serde_json::to_vec(self)?))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).context("Failed to deserialize SMS message")
    }

    /// NOTE: This still parses JSON fully — optimized parsing can be added later
    pub fn extract_conversation_id(bytes: &[u8]) -> Result<String> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        value["conversation_id"]
            .as_str()
            .map(|s| s.to_string())
            .context("Missing conversation_id field")
    }
}

/// -----------------------------
/// Lazy zero-copy message wrapper
/// -----------------------------
#[derive(Debug, Clone)]
pub struct LazyMessage {
    raw: Bytes,
    cached: Arc<Mutex<Option<SMSMessageView>>>,
}

impl LazyMessage {
    pub fn new(raw: Bytes) -> Self {
        Self {
            raw,
            cached: Arc::new(Mutex::new(None)),
        }
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.raw
    }

    pub async fn deserialize(&self) -> Result<SMSMessageView> {
        let mut guard = self.cached.lock().await;

        if let Some(msg) = guard.as_ref() {
            return Ok(msg.clone());
        }

        let msg = SMSMessageView::from_bytes(&self.raw)?;
        *guard = Some(msg.clone());
        Ok(msg)
    }

    pub async fn conversation_id(&self) -> Result<String> {
        SMSMessageView::extract_conversation_id(&self.raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_batch_creation() {
        let msg1 = b"Hello";
        let msg2 = b"World";
        let msg3 = b"ZeroCopy";

        let batch = MessageBatch::from_messages(&[msg1, msg2, msg3]);

        assert_eq!(batch.count, 3);
        assert_eq!(&batch.get(0).unwrap()[..], msg1);
        assert_eq!(&batch.get(1).unwrap()[..], msg2);
        assert_eq!(&batch.get(2).unwrap()[..], msg3);
    }

    #[test]
    fn test_message_batch_iterator() {
        let messages = vec![b"a".as_slice(), b"bb".as_slice(), b"ccc".as_slice()];
        let batch = MessageBatch::from_messages(&messages);

        let collected: Vec<_> = batch.iter().collect();
        assert_eq!(&collected[0][..], b"a");
        assert_eq!(&collected[1][..], b"bb");
        assert_eq!(&collected[2][..], b"ccc");
    }
}
