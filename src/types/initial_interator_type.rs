use crate::types::checkpoint::Checkpoint;

#[derive(Debug, Clone)]
pub enum InitialIteratorType {
    Latest,
    TrimHorizon,
    AtCheckpoint(Checkpoint),
    AfterCheckpoint(Checkpoint),
}
