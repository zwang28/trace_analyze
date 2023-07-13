use std::collections::{BTreeSet, HashMap};
use base64::Engine;

fn main() {
    let csv_path = "trace.csv";
    let target_table_id = "1007";
    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut min_timestamp: Option<u64> = None;
    let mut max_timestamp: Option<u64> = None;
    let max_ts_bucket_num: u64 = 1000;

    let mut reader = csv::ReaderBuilder::new().from_path(csv_path).unwrap();
    for r in reader.records() {
        let r = r.unwrap();
        let table_id = r.get(0).unwrap();
        if target_table_id != table_id {
            continue;
        }
        let data = r.get(1).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD.decode(data).unwrap();
        let timestamp = str::parse::<u64>(r.get(2).unwrap()).unwrap();
        keys.insert(decoded);
        if min_timestamp.is_none() {
            min_timestamp = Some(timestamp);
        }
        max_timestamp = Some(timestamp);
    }
    let min_timestamp = min_timestamp.unwrap();
    let max_timestamp = max_timestamp.unwrap();
    let mut ts_bucket_size_sec: u64 = (max_timestamp - min_timestamp + 1) / max_ts_bucket_num;
    if (max_timestamp - min_timestamp + 1) % max_ts_bucket_num != 0 {
        ts_bucket_size_sec += 1;
    }

    let mut key_seq_map: HashMap<Vec<u8>, u64> = HashMap::with_capacity(keys.len());
    let mut key_seq_count = 0;
    while let Some(k) = keys.pop_first() {
        key_seq_map.insert(k, key_seq_count);
        key_seq_count += 1;
    }

    println!("key_seq_count={} bucket_count={} ts_bucket_size_sec={}", key_seq_count, max_ts_bucket_num, ts_bucket_size_sec);

    let mut reader = csv::ReaderBuilder::new().from_path(csv_path).unwrap();
    for r in reader.records() {
        let r = r.unwrap();
        let table_id = r.get(0).unwrap();
        if target_table_id != table_id {
            continue;
        }
        let data = r.get(1).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD.decode(data).unwrap();
        let timestamp = str::parse::<u64>(r.get(2).unwrap()).unwrap();
        let key_seq_id = *key_seq_map.get(&decoded).unwrap();
        let ts_bucket_id = (timestamp - min_timestamp) / ts_bucket_size_sec;
    }
}
