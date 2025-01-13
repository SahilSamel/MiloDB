pub mod sstable;
pub mod compaction;
pub mod test; 
/*
Things to do:
1.figure out if we can extract timestamp based on byte position instead of json field
2.namings of the sstables
3.finalizing the inputs based on the upsteam(skiplist/btree)
4. Fix the file overwrites
 */