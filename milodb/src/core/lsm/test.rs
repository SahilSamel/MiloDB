use serde_json::{json, Value};
use crate::core::lsm::sstable::SSTable;

pub fn main() {
    use std::collections::BTreeMap;

    let mut data = BTreeMap::new();

    // Example JSON messages
    let message1 = json!({
        "message_id": "msg123",
        "timestamp": "2024-12-25T14:30:00Z",
        "chat_room_id": "room456",
        "sender_id": "user789",
        "recipient_id": "user012",
        "message": "Hello, how are you?",
        "metadata": {
            "is_edited": false,
            "is_deleted": false
        }
    });
    let message2 = json!({
        "message_id": "msg124",
        "timestamp": "2024-12-25T15:00:00Z",
        "chat_room_id": "room457",
        "sender_id": "user790",
        "recipient_id": "user013",
        "message": "Hi! I'm good, thanks.",
        "metadata": {
            "is_edited": false,
            "is_deleted": false
        }
    });

    // Insert data into BTreeMap
    data.insert(b"msg123".to_vec(), serde_json::to_vec(&message1).unwrap());
    data.insert(b"msg124".to_vec(), serde_json::to_vec(&message2).unwrap());

    // Write SSTable
    let sstable = SSTable::write(&data, "sstable_test.dat").unwrap();
    println!(
        "SSTable written with timestamp range: {:?}",
        sstable.timestamp_range
    );

    // Read SSTable
    let read_data = sstable.read().unwrap();
    for (key, value) in read_data {
        println!("Key: {:?}", String::from_utf8(key).unwrap());
        println!("Value: {:?}", serde_json::from_slice::<Value>(&value).unwrap());
    }
}
