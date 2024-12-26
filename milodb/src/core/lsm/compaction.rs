use std::collections::BTreeMap; use std::fs::File;
// For BTreeMap
use std::io::{self, Write}; // For std::io::Result
use crate::core::lsm::sstable::SSTable; // Import the SSTable struct from sstable.rs


pub fn compact<K: Ord,V>(sstables: Vec<SSTable>, output_path: &str) -> std::io::Result<()>
where
    K:Ord + AsRef<[u8]>,
    V:AsRef<[u8]>,
    {
        let mut merged_data = BTreeMap::new();

        for sstable in sstables.iter(){
            let data= sstable.read()?;
            for (key, value) in data {
                merged_data.insert(key,value);
            }
        }

        let output_sstable =SSTable::new(output_path);
        let mut file = File::create(output_path)?; // Create the output file
        for (key, value) in merged_data {
            file.write_all(&key)?; // Write key
            file.write_all(&value)?; // Write value
        }


        Ok(())
    }