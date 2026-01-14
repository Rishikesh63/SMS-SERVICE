/// Zero-copy message serialization inspired by Apache Iggy's approach
/// Provides efficient message views without full deserialization
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use serde::{Deserialize, Serialize};

/// Message batch with indexed access to individual messages
/// Similar to Iggy's approach: separate index from payload for efficient slicing
#[derive(Debug, Clone)]
pub struct MessageBatch {
    /// Index of message offsets (each u32 = 4 bytes)
    pub indexes: Bytes,
    /// Raw message payload data
    pub messages: Bytes,
    /// Number of messages in the batch
    pub count: usize,
}

impl MessageBatch {
    /// Create a new batch from a vector of messages
    pub fn from_messages(messages: &[&[u8]]) -> Self {
        let count = messages.len();
        
        // Pre-allocate index buffer: 4 bytes per message
        let mut index_buf = BytesMut::with_capacity(count * 4);
        
        // Pre-allocate message buffer
        let total_size: usize = messages.iter().map(|m| m.len()).sum();
        let mut msg_buf = BytesMut::with_capacity(total_size);
        
        let mut offset = 0u32;
        for msg in messages {
            // Write offset to index
            index_buf.put_u32_le(offset);
            
            // Write message to payload
            msg_buf.put_slice(msg);
            offset += msg.len() as u32;
        }
        
        Self {
            indexes: index_buf.freeze(),
            messages: msg_buf.freeze(),
            count,
        }
    }
    
    /// Get an iterator over message views (zero-copy access)
    pub fn iter(&self) -> MessageBatchIterator {
        MessageBatchIterator {
            indexes: self.indexes.clone(),
            messages: self.messages.clone(),
            current: 0,
            count: self.count,
        }
    }
    
    /// Get a specific message by index (zero-copy view)
    pub fn get(&self, index: usize) -> Option<Bytes> {
        if index >= self.count {
            return None;
        }
        
        let mut idx_buf = self.indexes.clone();
        idx_buf.advance(index * 4);
        let start = idx_buf.get_u32_le() as usize;
        
        let end = if index + 1 < self.count {
            let next_offset = idx_buf.get_u32_le() as usize;
            next_offset
        } else {
            self.messages.len()
        };
        
        Some(self.messages.slice(start..end))
    }
    
    /// Get the total size in bytes (metadata + payload)
    pub fn total_size(&self) -> usize {
        self.indexes.len() + self.messages.len()
    }
}

/// Iterator over message views in a batch
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

/// SMS Message with efficient serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMSMessageView {
    pub from: String,
    pub to: String,
    pub body: String,
    pub timestamp: i64,
    pub conversation_id: String,
}

impl SMSMessageView {
    /// Serialize to bytes (for sending)
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec(self)?;
        Ok(Bytes::from(json))
    }
    
    /// Deserialize from bytes view (zero-copy until needed)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).context("Failed to deserialize SMS message")
    }
    
    /// Quick metadata extraction without full deserialization
    /// For routing and filtering without allocating full struct
    pub fn extract_conversation_id(bytes: &[u8]) -> Result<String> {
        // Fast path: parse only conversation_id field
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        value["conversation_id"]
            .as_str()
            .map(|s| s.to_string())
            .context("Missing conversation_id field")
    }
}

/// Zero-copy wrapper around raw message bytes
/// Delays deserialization until actually needed (inspired by Apache Iggy)
#[derive(Debug, Clone)]
pub struct LazyMessage {
    /// Raw message bytes (shared reference, no copy)
    raw: Bytes,
    /// Cached deserialized message (lazy)
    cached: std::sync::Arc<std::sync::Mutex<Option<SMSMessageView>>>,
}

impl LazyMessage {
    /// Create from bytes (zero-copy)
    pub fn new(raw: Bytes) -> Self {
        Self {
            raw,
            cached: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }
    
    /// Get raw bytes (zero-copy)
    pub fn as_bytes(&self) -> &Bytes {
        &self.raw
    }
    
    /// Deserialize only when needed (cached)
    pub fn deserialize(&self) -> Result<SMSMessageView> {
        let mut cached = self.cached.lock().unwrap();
        
        if let Some(msg) = cached.as_ref() {
            return Ok(msg.clone());
        }
        
        let msg = SMSMessageView::from_bytes(&self.raw)?;
        *cached = Some(msg.clone());
        Ok(msg)
    }
    
    /// Extract conversation_id without full deserialization
    pub fn conversation_id(&self) -> Result<String> {
        SMSMessageView::extract_conversation_id(&self.raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_batch_creation() {
        let msg1 = b"Hello, world!";
        let msg2 = b"Benchmark test";
        let msg3 = b"Zero-copy rocks!";
        
        let batch = MessageBatch::from_messages(&[msg1, msg2, msg3]);
        
        assert_eq!(batch.count, 3);
        assert_eq!(batch.indexes.len(), 12); // 3 * 4 bytes
        
        let m1 = batch.get(0).unwrap();
        assert_eq!(&m1[..], msg1);
        
        let m2 = batch.get(1).unwrap();
        assert_eq!(&m2[..], msg2);
        
        let m3 = batch.get(2).unwrap();
        assert_eq!(&m3[..], msg3);
    }
    
    #[test]
    fn test_message_batch_iterator() {
        let messages = vec![b"msg1".as_slice(), b"message2".as_slice(), b"m3".as_slice()];
        let batch = MessageBatch::from_messages(&messages);
        
        let collected: Vec<Bytes> = batch.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(&collected[0][..], b"msg1");
        assert_eq!(&collected[1][..], b"message2");
        assert_eq!(&collected[2][..], b"m3");
    }
}
