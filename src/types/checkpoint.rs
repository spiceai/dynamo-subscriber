use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub shard_sequence_numbers: HashMap<String, String>,
}
