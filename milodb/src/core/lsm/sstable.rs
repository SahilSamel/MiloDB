use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::io::BufWriter;
use serde_json::Value;


#[derive(Clone)]
pub struct SSTable {
    pub file_path: String,          // Path to the SSTable file.
    pub timestamp_range: (u64, u64), // Timestamp range covered by this SSTable.
}

impl SSTable {
    /// Create a new SSTable instance with the given file path and timestamp range.
    pub fn new(file_path: &str, timestamp_range: (u64, u64)) -> Self {
        Self {
            file_path: file_path.to_string(),
            timestamp_range,
        }
    }

    /// Write data from any source implementing the `DataSource` trait to an SSTable.
    pub fn write<D: DataSource>(source: &D, file_path: &str) -> io::Result<Self> {
        let mut file = BufWriter::new(File::create(file_path)?);
        let mut min_timestamp = u64::MAX;
        let mut max_timestamp = u64::MIN;

        for (key, value) in source.iter() {
            // Parse the JSON value to extract the timestamp
            let value_json: Value = serde_json::from_slice(&value).unwrap();
            let timestamp_str = value_json["timestamp"]
                .as_str()
                .expect("Timestamp should be a string");
            let timestamp = Self::parse_timestamp(timestamp_str);

            min_timestamp = min_timestamp.min(timestamp);
            max_timestamp = max_timestamp.max(timestamp);

            // Write the key and value
            file.write_all(&(key.len() as u32).to_be_bytes())?; // Write key length
            file.write_all(&key)?;                              // Write key
            file.write_all(&(value.len() as u32).to_be_bytes())?; // Write value length
            file.write_all(&value)?;                            // Write value
        }

        Ok(Self::new(file_path, (min_timestamp, max_timestamp)))
    }

    /// Read all key-value pairs from the SSTable.
    pub fn read(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let file = BufReader::new(File::open(&self.file_path)?);
        let mut reader = file;
        let mut data = BTreeMap::new();

        loop {
            // Read key length (u32 - 4 bytes)
            let mut key_len_buf = [0u8; 4];
            if reader.read_exact(&mut key_len_buf).is_err() {
                break; // EOF or error
            }
            let key_len = u32::from_be_bytes(key_len_buf) as usize;

            // Read the key
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            // Read value length (u32 - 4 bytes)
            let mut value_len_buf = [0u8; 4];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_be_bytes(value_len_buf) as usize;

            // Read the value
            let mut value = vec![0u8; value_len];
            reader.read_exact(&mut value)?;

            // Insert the key-value pair into the BTreeMap
            data.insert(key, value);
        }

        Ok(data)
    }

    /// Helper function to parse ISO 8601 timestamp into a Unix timestamp
    fn parse_timestamp(timestamp: &str) -> u64 {
        use chrono::{DateTime, NaiveDateTime, Utc};

        let datetime: DateTime<Utc> = DateTime::parse_from_rfc3339(timestamp)
            .expect("Failed to parse ISO 8601 timestamp")
            .with_timezone(&Utc);

        datetime.timestamp() as u64
    }
}

/// Define a trait for generic data sources
pub trait DataSource {
    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>;
}

/// Implement `DataSource` for BTreeMap
impl DataSource for BTreeMap<Vec<u8>, Vec<u8>> {
    fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_> {
        Box::new(self.iter().map(|(k, v)| (k.clone(), v.clone())))
    }
}
