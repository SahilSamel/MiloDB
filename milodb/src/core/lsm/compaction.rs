use std::io;
use std::collections::{BTreeMap, HashMap};
use crate::core::lsm::sstable::{SSTable, DataSource};

/// Struct to manage SSTable tiers for compaction.
pub struct TieredStorage {
    pub tiers: HashMap<u64, Vec<SSTable>>, // SSTables organized by tier.
    pub tier_sizes: Vec<u64>,                // Dynamic sizes for each tier.
}

impl TieredStorage {
    /// Create a new instance of TieredStorage.
    pub fn new(tier_sizes: Vec<u64>) -> Self {
        Self {
            tiers: HashMap::new(),
            tier_sizes,
        }
    }

    /// Add a new SSTable to the appropriate tier based on its timestamp.
    pub fn add_sstable(&mut self, sstable: SSTable) {
        let timestamp = sstable.timestamp_range.1; // Use the max timestamp.
        let mut assigned_tier:u64 = 0;

        // Assign the SSTable to the appropriate tier.
        for (i, &tier_size) in self.tier_sizes.iter().enumerate() {
            if timestamp <= tier_size {
                assigned_tier = i as u64;
                break;
            }
        }

        self.tiers.entry(assigned_tier).or_insert_with(Vec::new).push(sstable);

        // Trigger compaction check for the tier.
        let _ = self.trigger_compaction(assigned_tier);
    }

    /// Trigger compaction if the tier exceeds a certain threshold.
    fn trigger_compaction(&mut self, tier: u64)-> io::Result<()> {
        
        
        // Temporarily take the SSTables for the tier out of the map.
        if let Some(mut sstables) = self.tiers.remove(&tier) {
            let mut size=0;
            println!("{}", sstables.len());
            if sstables.len() > 4 { // Example threshold
                println!("Compacting tier {}...", tier);
    
                // Perform compaction.
                let compacted_sstable = self.compact_tier(sstables.clone(), tier).unwrap();
                size=SSTable::count_messages_in_sstable(&compacted_sstable.file_path)? as u64;
                println!("the size is {}",size);
    
                // Clear the tier and add the compacted SSTable back.
                sstables.clear();
                sstables.push(compacted_sstable);
            }
    
            // Reinsert the updated SSTables back into the map.
            if size > 10{
                self.tiers.insert(2, sstables);
            }
            else if size>5 {
                self.tiers.insert(1, sstables);
            }
            else{
                self.tiers.insert(0, sstables);
            }
            
        }
        Ok(())
        
    }

    /// Compact all SSTables in a given tier into one SSTable.
    fn compact_tier(&self, sstables: Vec<SSTable>, tier: u64) -> std::io::Result<SSTable> {
        let mut merged_data = BTreeMap::new();
        let mut count:u64=0;
        for sstable in sstables {
            let data = sstable.read()?;
            for (key, value) in data {
                merged_data.insert(key, value);
                count += 1;
            }
        }
        for (key, value) in &self.tiers {
            println!("Key: {}", key); // Assuming key is an integer.
            println!("Value: {:?}", value); // Assuming value is a vector.
        }
        let tier_to_be_inserted:u64= Self::find_tier(count);
        let output_path;
        if tier==tier_to_be_inserted {
            output_path = format!("tier_{}_0.sst",tier);
        }else{
            let no_of_sstables_in_tier = if self.tiers.contains_key(&tier_to_be_inserted) {
            
                self.tiers[&tier_to_be_inserted].len() + 1
            } else {
                println!("here");
                1
            };

            output_path = format!("tier_{}_{}.sst",tier_to_be_inserted,no_of_sstables_in_tier);
        }
        
        //println!("{}:{}",tier,no_of_sstables_in_tier);
       
        SSTable::write(&merged_data, &output_path)
    }

    /// Add data from any source implementing the `DataSource` trait, converting it to an SSTable.
    pub fn add_data_source<D: DataSource>(&mut self, source: D, file_path: &str) -> std::io::Result<()> {
        let sstable = SSTable::write(&source, file_path)?;
        self.add_sstable(sstable);
        Ok(())
    }

    pub fn find_tier(count:u64)-> u64{
        if count>10{
            2
       }
       else if count>5{
           1
       }
       else {
            0
       }
    }
    
}
