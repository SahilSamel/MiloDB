use std::collections::BTreeMap; // For BTreeMap
use std::fs::{File, OpenOptions}; // For file operations
use std::io::{BufRead, BufReader, BufWriter, Read, Write}; // For buffered reading/writing


#[derive(Debug, Clone)]
pub struct SSTable{
    file_path:String,
}

impl SSTable{
    pub fn new(file_path: &str)->Self {
        SSTable{
            file_path:file_path.to_string(),
        }
    }

    pub fn write<K,V>(&self, data: &BTreeMap<K,V>) -> std::io::Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&self.file_path)?;
        let mut writer = BufWriter::new(file);

        for (key,value) in data.iter(){
            writer.write_all(key.as_ref())?;
            writer.write_all(b"\n")?;
            writer.write_all(value.as_ref())?;
            writer.write_all(b"\n")?;
        } 
        writer.flush()?;
        Ok(())
    }

    pub fn read(&self) -> std::io::Result<BTreeMap<Vec<u8>,Vec<u8>>>{
        let file= File::open(&self.file_path)?;
        let mut reader= BufReader::new(file);
        let mut data= BTreeMap::new();
        let mut key = Vec::new();
        let mut value= Vec::new();

        loop{
            key.clear();
            value.clear();

            if reader.read_until(b'\n', &mut key)? ==0 {
                break;
            }
            
            reader.read_until(b'\n', &mut value)?;

            if let Some(_) = key.last(){
                key.pop();
            }
            if let Some(_)= value.last(){
                value.pop();
            }
            data.insert(key.clone(),value.clone());

            
        }
        Ok(data)
    }
}