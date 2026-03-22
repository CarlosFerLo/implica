use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::OnceCell;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::Uid;
use crate::utils::{CreationCounter, SlotGuard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchElement {
    Type(Uid),
    Term(Uid),
    Node(Uid),
    Edge((Uid, Uid)),
}

impl MatchElement {
    pub fn as_type(&self, var: &str, context: Option<String>) -> ImplicaResult<Uid> {
        match self {
            MatchElement::Type(t) => Ok(*t),
            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "term".to_string(),
                new: "type".to_string(),
                context,
            }
            .into()),
            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "node".to_string(),
                new: "type".to_string(),
                context,
            }
            .into()),
            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "edge".to_string(),
                new: "type".to_string(),
                context,
            }
            .into()),
        }
    }
    pub fn as_term(&self, var: &str, context: Option<String>) -> ImplicaResult<Uid> {
        match self {
            MatchElement::Term(t) => Ok(*t),
            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "type".to_string(),
                new: "term".to_string(),
                context,
            }
            .into()),
            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "node".to_string(),
                new: "term".to_string(),
                context,
            }
            .into()),
            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "edge".to_string(),
                new: "term".to_string(),
                context,
            }
            .into()),
        }
    }
    pub fn as_node(&self, var: &str, context: Option<String>) -> ImplicaResult<Uid> {
        match self {
            MatchElement::Node(t) => Ok(*t),
            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "type".to_string(),
                new: "node".to_string(),
                context,
            }
            .into()),
            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "term".to_string(),
                new: "node".to_string(),
                context,
            }
            .into()),
            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "edge".to_string(),
                new: "node".to_string(),
                context,
            }
            .into()),
        }
    }
    pub fn as_edge(&self, var: &str, context: Option<String>) -> ImplicaResult<(Uid, Uid)> {
        match self {
            MatchElement::Edge(t) => Ok(*t),
            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "type".to_string(),
                new: "edge".to_string(),
                context,
            }
            .into()),
            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "term".to_string(),
                new: "edge".to_string(),
                context,
            }
            .into()),
            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                name: var.to_string(),
                original: "node".to_string(),
                new: "edge".to_string(),
                context,
            }
            .into()),
        }
    }
}

#[derive(Debug)]
pub struct Match {
    previous: Option<Arc<Match>>,
    elements: Arc<DashMap<String, MatchElement>>,

    creation_counter: OnceCell<CreationCounter>,
}

impl Match {
    pub fn new(previous: Option<Arc<Match>>) -> Self {
        Match {
            previous,
            elements: Arc::new(DashMap::new()),
            creation_counter: OnceCell::new(),
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

    pub fn insert(&self, key: &str, element: MatchElement) -> ImplicaResult<()> {
        if self.contains_key(key) {
            return Err(ImplicaError::VariableAlreadyExists {
                name: key.to_string(),
                context: Some((ctx!("match insert")).to_string()),
            }
            .into());
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

    pub fn set_creation_limit(&self, limit: u32) {
        let _ = self.creation_counter.set(CreationCounter::new(limit)); // Ignores if already set
    }

    pub fn check_counter(&self) -> bool {
        if let Some(counter) = self.creation_counter.get() {
            counter.is_available()
        } else if let Some(ref previous) = self.previous {
            previous.check_counter()
        } else {
            true
        }
    }

    pub fn get_counter(&self) -> ImplicaResult<Option<SlotGuard<'_>>> {
        if let Some(counter) = self.creation_counter.get() {
            Ok(counter.acquire())
        } else if let Some(ref previous) = self.previous {
            previous.get_counter()
        } else {
            Err(ImplicaError::CreationCounterNotFound {
                context: Some("match - get counter".to_string()),
            }
            .into())
        }
    }
}

pub type MatchSet = Arc<DashMap<u64, (Uid, Arc<Match>)>>;

pub(crate) fn default_match_set() -> MatchSet {
    let mset = Arc::new(DashMap::new());
    mset.insert(0, ([0; 32], Arc::new(Match::new(None))));
    mset
}
