pub mod sstable;
pub mod compaction;
pub mod test; 

/*
Things to do:
1.figure out if we can extract timestamp based on byte position instead of json field
2.finalizing the inputs based on the upsteam(skiplist/btree)
3.Fix and optimize the compaction triggering tthreshold. 
4.While writing an SSTABLE it should check the curr size of tier 0 and based on that insert it
 */