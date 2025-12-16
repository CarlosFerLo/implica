use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use crate::{errors::ImplicaError, graph::Uid};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchElement {
    Type(Uid),
    Term(Uid),
    Node(Uid),
    Edge((Uid, Uid)),
}

#[derive(Debug, Clone)]
pub struct Match {
    previous: Option<Arc<Match>>,
    elements: Arc<DashMap<String, MatchElement>>,
}

impl Match {
    pub fn new(previous: Option<Arc<Match>>) -> Self {
        Match {
            previous,
            elements: Arc::new(DashMap::new()),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        if let Some(ref previous) = self.previous {
            if previous.contains_key(key) {
                return true;
            }
        }

        self.elements.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<MatchElement> {
        if let Some(ref previous) = self.previous {
            if let Some(element) = previous.get(key) {
                return Some(element);
            }
        }

        self.elements.get(key).map(|e| e.value().clone())
    }

    pub fn insert(&self, key: &str, element: MatchElement) -> Result<(), ImplicaError> {
        if self.contains_key(key) {
            return Err(ImplicaError::VariableAlreadyExists {
                name: key.to_string(),
                context: Some("match insert".to_string()),
            });
        }

        self.elements.insert(key.to_string(), element);
        Ok(())
    }
}

pub type MatchSet = Arc<DashMap<u64, (Uid, Arc<Match>)>>;

pub static MATCH_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn next_match_id() -> u64 {
    MATCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}
