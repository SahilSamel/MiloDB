use crate::core::lsm::sstable::SSTable; // Import the SSTable struct
use crate::core::lsm::compaction; // Import the compact function
use std::collections::BTreeMap; // For BTreeMap
use std::io::{self, Write}; // For std::io::Result
use std::fs::File; // For File::create
use std::io::BufWriter; // For BufWriter::new


pub fn write_test_data(file_path: &str, data: &BTreeMap<Vec<u8>, Vec<u8>>) -> std::io::Result<()> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    for (key, value) in data {
        writer.write_all(key)?;
        writer.write_all(b"\n")?;
        writer.write_all(value)?;
        writer.write_all(b"\n")?;
    }

    Ok(())
}

pub fn main() -> std::io::Result<()> {
    // Step 1: Prepare test data
    let mut sstable1_data = BTreeMap::new();
    sstable1_data.insert(b"Key1".to_vec(), b"Value1".to_vec());
    sstable1_data.insert(b"Key2".to_vec(), b"Value2".to_vec());

    let mut sstable2_data = BTreeMap::new();
    sstable2_data.insert(b"Key2".to_vec(), b"Value3".to_vec());
    sstable2_data.insert(b"Key3".to_vec(), b"Value4".to_vec());

    // Step 2: Write to SSTable files
    write_test_data("sstable1.txt", &sstable1_data)?;
    write_test_data("sstable2.txt", &sstable2_data)?;

    // Step 3: Create SSTable objects
    let sstable1 = SSTable::new("sstable1.txt");
    let sstable2 = SSTable::new("sstable2.txt");

    // Step 4: Perform compaction
    crate::core::lsm::compaction::compact::<Vec<u8>, Vec<u8>>(vec![sstable1, sstable2], "compacted_sstable.txt")?;

    // Step 5: Verify output
    let compacted = SSTable::new("compacted_sstable.txt");
    let compacted_data = compacted.read()?;

    println!("Compacted SSTable Data:");
    for (key, value) in compacted_data {
        println!("{:?} -> {:?}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
    }

    Ok(())
}
