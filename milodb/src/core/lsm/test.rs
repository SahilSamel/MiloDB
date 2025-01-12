use std::collections::BTreeMap;

use crate::core::lsm::sstable::{SSTable, DataSource};
use crate::core::lsm::compaction::TieredStorage;

pub fn main() -> std::io::Result<()> {
    let tier_sizes = vec![1, 5, 10]; // Example tier size ranges.
    let mut storage = TieredStorage::new(tier_sizes);

    use chrono::{Utc, Duration};

for i in 1..=6 {
    let mut data = BTreeMap::new();

    // Generate ISO 8601 timestamp.
    let timestamp = Utc::now() + Duration::seconds(i as i64 * 100); // Increment timestamps.
    let timestamp_str = timestamp.to_rfc3339(); // Convert to ISO 8601 format.

    let key = format!("msg{}", i).as_bytes().to_vec();
    let value = serde_json::to_vec(&serde_json::json!({
        "message_id": format!("msg{}", i),
        "timestamp": timestamp_str, // Pass timestamp as a valid ISO 8601 string.
        "chat_room_id": "room456",
        "sender_id": format!("user{}", i),
        "recipient_id": "user012",
        "message": format!("Message {}", i),
        "metadata": {
            "is_edited": false,
            "is_deleted": false
        }
    }))?;

    data.insert(key.clone(), value);

    // Add data as SSTable.
    let file_path = format!("sstable_{}.sst", i);
    storage.add_data_source(data, &file_path)?;
    }
    // Add sample data as SSTables.
    

    // Print out tiers after compaction.
    println!("Tiers after compaction:");
    for (tier, sstables) in &storage.tiers {
        println!("Tier {}: {} SSTables", tier, sstables.len());
        for sstable in sstables {
            println!(
                "  SSTable {} -> Timestamp Range: {:?}",
                sstable.file_path, sstable.timestamp_range
                
            );
            let count = SSTable::count_messages_in_sstable(&sstable.file_path)?;
                println!("Number of messages in the SSTable: {}", count);
        }
    }

    Ok(())
}
