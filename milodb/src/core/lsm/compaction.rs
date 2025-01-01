use std::collections::{BTreeMap, HashMap};
use crate::core::lsm::sstable::{SSTable, DataSource};

/// Struct to manage SSTable tiers for compaction.
pub struct TieredStorage {
    pub tiers: HashMap<usize, Vec<SSTable>>, // SSTables organized by tier.
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
        let mut assigned_tier = 0;

        // Assign the SSTable to the appropriate tier.
        for (i, &tier_size) in self.tier_sizes.iter().enumerate() {
            if timestamp <= tier_size {
                assigned_tier = i;
                break;
            }
        }

        self.tiers.entry(assigned_tier).or_insert_with(Vec::new).push(sstable);

        // Trigger compaction check for the tier.
        self.trigger_compaction(assigned_tier);
    }

    /// Trigger compaction if the tier exceeds a certain threshold.
    fn trigger_compaction(&mut self, tier: usize) {
        // Temporarily take the SSTables for the tier out of the map.
        if let Some(mut sstables) = self.tiers.remove(&tier) {
            if sstables.len() > 3 { // Example threshold
                println!("Compacting tier {}...", tier);
    
                // Perform compaction.
                let compacted_sstable = self.compact_tier(sstables.clone(), tier).unwrap();
    
                // Clear the tier and add the compacted SSTable back.
                sstables.clear();
                sstables.push(compacted_sstable);
            }
    
            // Reinsert the updated SSTables back into the map.
            self.tiers.insert(tier, sstables);
        }
    }

    /// Compact all SSTables in a given tier into one SSTable.
    fn compact_tier(&self, sstables: Vec<SSTable>, tier: usize) -> std::io::Result<SSTable> {
        let mut merged_data = BTreeMap::new();

        for sstable in sstables {
            let data = sstable.read()?;
            for (key, value) in data {
                merged_data.insert(key, value);
            }
        }

        let output_path = format!("tier_{}_compacted.sst", tier);
        SSTable::write(&merged_data, &output_path)
    }

    /// Add data from any source implementing the `DataSource` trait, converting it to an SSTable.
    pub fn add_data_source<D: DataSource>(&mut self, source: D, file_path: &str) -> std::io::Result<()> {
        let sstable = SSTable::write(&source, file_path)?;
        self.add_sstable(sstable);
        Ok(())
    }
}
