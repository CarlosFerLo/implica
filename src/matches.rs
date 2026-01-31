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

impl MatchElement {
    pub fn as_type(&self, var: &str, context: Option<String>) -> Result<Uid, ImplicaError> {
        match self {
            MatchElement::Type(t) => Ok(*t),
            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "term".to_string(),
                new: "type".to_string(),
                context,
            }),
            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "node".to_string(),
                new: "type".to_string(),
                context,
            }),
            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "edge".to_string(),
                new: "type".to_string(),
                context,
            }),
        }
    }
    pub fn as_term(&self, var: &str, context: Option<String>) -> Result<Uid, ImplicaError> {
        match self {
            MatchElement::Term(t) => Ok(*t),
            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "type".to_string(),
                new: "term".to_string(),
                context,
            }),
            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "node".to_string(),
                new: "term".to_string(),
                context,
            }),
            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "edge".to_string(),
                new: "term".to_string(),
                context,
            }),
        }
    }
    pub fn as_node(&self, var: &str, context: Option<String>) -> Result<Uid, ImplicaError> {
        match self {
            MatchElement::Node(t) => Ok(*t),
            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "type".to_string(),
                new: "node".to_string(),
                context,
            }),
            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "term".to_string(),
                new: "node".to_string(),
                context,
            }),
            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "edge".to_string(),
                new: "node".to_string(),
                context,
            }),
        }
    }
    pub fn as_edge(&self, var: &str, context: Option<String>) -> Result<(Uid, Uid), ImplicaError> {
        match self {
            MatchElement::Edge(t) => Ok(*t),
            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "type".to_string(),
                new: "edge".to_string(),
                context,
            }),
            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "term".to_string(),
                new: "edge".to_string(),
                context,
            }),
            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "node".to_string(),
                new: "edge".to_string(),
                context,
            }),
        }
    }
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

    pub fn remove(&self, key: &str) -> Option<MatchElement> {
        if let Some((_, element)) = self.elements.remove(key) {
            Some(element)
        } else if let Some(previous) = &self.previous {
            previous.remove(key)
        } else {
            None
        }
    }
}

pub type MatchSet = Arc<DashMap<u64, (Uid, Arc<Match>)>>;

pub static MATCH_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn next_match_id() -> u64 {
    MATCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}
