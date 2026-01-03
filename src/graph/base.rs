use rayon::iter::IntoParallelRefIterator;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use rayon::prelude::*;

use crate::errors::ImplicaError;
use crate::matches::{next_match_id, Match, MatchSet};
use crate::typing::{Term, Type};

#[path = "matches/edge.rs"]
mod __edge_pattern;
#[path = "matches/node.rs"]
mod __node_pattern;
#[path = "matches/path.rs"]
mod __path_pattern;
#[path = "matches/term_schema.rs"]
mod __term_schema;
#[path = "matches/type_schema.rs"]
mod __type_schema;

pub type Uid = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq)]
enum TypeRep {
    Variable(String),
    Arrow(Uid, Uid),
}

impl TypeRep {
    pub fn uid(&self) -> Uid {
        match self {
            TypeRep::Variable(name) => {
                let mut hasher = Sha256::new();
                hasher.update(b"var:");
                hasher.update(name.as_bytes());
                hasher.finalize().into()
            }
            TypeRep::Arrow(left, right) => {
                let mut hasher = Sha256::new();
                hasher.update(b"arr:");
                hasher.update(left);
                hasher.update(b":");
                hasher.update(right);
                hasher.finalize().into()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TermRep {
    Base(String),
    Application(Uid, Uid),
}

type NodeSet = Arc<DashSet<Uid>>;
type EdgeSet = Arc<DashSet<(Uid, Uid)>>;

#[derive(Clone, Debug)]
pub struct Graph {
    nodes: NodeSet,
    edges: EdgeSet,

    type_index: Arc<DashMap<Uid, TypeRep>>,
    term_index: Arc<DashMap<Uid, TermRep>>,

    type_to_edge_index: Arc<DashMap<Uid, (Uid, Uid)>>,
    edge_to_type_index: Arc<DashMap<(Uid, Uid), Uid>>,

    start_to_edge_index: Arc<DashMap<Uid, EdgeSet>>,
    end_to_edge_index: Arc<DashMap<Uid, EdgeSet>>,
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            nodes: Arc::new(DashSet::new()),
            edges: Arc::new(DashSet::new()),
            type_index: Arc::new(DashMap::new()),
            term_index: Arc::new(DashMap::new()),
            type_to_edge_index: Arc::new(DashMap::new()),
            edge_to_type_index: Arc::new(DashMap::new()),
            start_to_edge_index: Arc::new(DashMap::new()),
            end_to_edge_index: Arc::new(DashMap::new()),
        }
    }

    pub fn add_node(&self, r#type: Type, term: Option<Term>) -> Result<Uid, ImplicaError> {
        let type_uid = self.insert_type(r#type);

        if let Some(term) = term {
            self.insert_term(term);
        }

        self.nodes.insert(type_uid);
        self.start_to_edge_index
            .insert(type_uid, Arc::new(DashSet::new()));
        self.end_to_edge_index
            .insert(type_uid, Arc::new(DashSet::new()));

        Ok(type_uid)
    }

    pub fn add_edge(&self, term: Term) -> Result<(Uid, Uid), ImplicaError> {
        let term_uid = self.insert_term(term);

        let edge_uid = if let Some(ref type_rep) = self.type_index.get(&term_uid) {
            match type_rep.value() {
                TypeRep::Arrow(left, right) => (*left, *right),
                TypeRep::Variable(_) => {
                    return Err(ImplicaError::InvalidTerm {
                        reason: "to create an edge you must provide a term of an arrow type"
                            .to_string(),
                    });
                }
            }
        } else {
            return Err(ImplicaError::RuntimeError {
                message: "unable to get term rep of a just initialized term".to_string(),
                context: Some("new edge".to_string()),
            });
        };

        self.type_to_edge_index.insert(term_uid, edge_uid);
        self.edge_to_type_index.insert(edge_uid, term_uid);

        if let Some(start_to_edge_index) = self.start_to_edge_index.get(&edge_uid.0) {
            let index = start_to_edge_index.value().clone();

            index.insert(edge_uid);
        } else {
            return Err(ImplicaError::IndexCorruption {
                message: "start_to_edge_index not initialized for some node already in the graph"
                    .to_string(),
                context: Some("add edge".to_string()),
            });
        }
        if let Some(end_to_edge_index) = self.end_to_edge_index.get(&edge_uid.1) {
            let index = end_to_edge_index.value().clone();

            index.insert(edge_uid);
        } else {
            return Err(ImplicaError::IndexCorruption {
                message: "end_to_edge_index not initialized for some node already in the graph"
                    .to_string(),
                context: Some("add edge".to_string()),
            });
        }

        self.edges.insert(edge_uid);

        Ok(edge_uid)
    }

    pub fn remove_node(&self, node_uid: &Uid) -> Result<Option<Uid>, ImplicaError> {
        if let Some(uid) = self.nodes.remove(node_uid) {
            let edges_to_remove: Vec<(Uid, Uid)> = self
                .edges
                .par_iter()
                .filter_map(|element| {
                    if uid == element.0 || uid == element.1 {
                        Some(*element)
                    } else {
                        None
                    }
                })
                .collect();

            for edge in edges_to_remove {
                self.remove_edge(&edge)?;
            }

            self.start_to_edge_index.remove(&uid);
            self.end_to_edge_index.remove(&uid);

            Ok(Some(uid))
        } else {
            Ok(None)
        }
    }

    pub fn remove_edge(&self, edge_uid: &(Uid, Uid)) -> Result<Option<(Uid, Uid)>, ImplicaError> {
        let uid = match self.edges.remove(edge_uid) {
            Some(uid) => uid,
            None => return Ok(None),
        };
        let (_, type_uid) = match self.edge_to_type_index.remove(edge_uid) {
            Some(pair) => pair,
            None => return Ok(None),
        };
        self.type_to_edge_index
            .remove(&type_uid)
            .ok_or(ImplicaError::IndexCorruption {
                message:
                    "type_to_edge_index lacks a pair that is contained in the edge_to_type_index"
                        .to_string(),
                context: Some("remove edge".to_string()),
            })?;

        if let Some(start_to_edge_index) = self.start_to_edge_index.get(&edge_uid.0) {
            let index = start_to_edge_index.value().clone();

            index.remove(edge_uid);
        } else {
            return Err(ImplicaError::IndexCorruption {
                message: "start_to_edge_index not initialized for some node already in the graph"
                    .to_string(),
                context: Some("add edge".to_string()),
            });
        }

        if let Some(end_to_edge_index) = self.end_to_edge_index.get(&edge_uid.1) {
            let index = end_to_edge_index.value().clone();

            index.remove(edge_uid);
        } else {
            return Err(ImplicaError::IndexCorruption {
                message: "start_to_edge_index not initialized for some node already in the graph"
                    .to_string(),
                context: Some("add edge".to_string()),
            });
        }

        Ok(Some(uid))
    }

    pub fn insert_type(&self, r#type: Type) -> Uid {
        match r#type {
            Type::Variable(var) => {
                let type_rep = TypeRep::Variable(var.name);
                let type_uid = type_rep.uid();

                self.type_index.insert(type_uid, type_rep);
                type_uid
            }
            Type::Arrow(arr) => {
                let left_uid = self.insert_type(arr.left.as_ref().clone());
                let right_uid = self.insert_type(arr.right.as_ref().clone());

                let type_rep = TypeRep::Arrow(left_uid, right_uid);
                let type_uid = type_rep.uid();

                self.type_index.insert(type_uid, type_rep);

                type_uid
            }
        }
    }

    pub fn insert_term(&self, term: Term) -> Uid {
        let term_type = term.r#type();
        let type_uid = self.insert_type(term_type.as_ref().clone());

        match term {
            Term::Basic(term) => {
                let term_rep = TermRep::Base(term.name);

                self.term_index.insert(type_uid, term_rep);
            }
            Term::Application(app) => {
                let function_uid = self.insert_term(app.function.as_ref().clone());
                let argument_uid = self.insert_term(app.argument.as_ref().clone());

                let term_rep = TermRep::Application(function_uid, argument_uid);
                self.term_index.insert(type_uid, term_rep);
            }
        }

        type_uid
    }

    pub fn start_match(&self) -> MatchSet {
        let map = Arc::new(DashMap::new());
        let initial_uid: Uid = [0u8; 32];
        map.insert(next_match_id(), (initial_uid, Arc::new(Match::new(None))));
        map
    }
}
