use rayon::iter::IntoParallelRefIterator;
use sha2::{Digest, Sha256};
use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use rayon::prelude::*;

use crate::errors::ImplicaError;
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::{NodePattern, TermPattern, TermSchema, TypePattern, TypeSchema};
use crate::typing::{Term, Type};

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

#[derive(Clone, Debug)]
pub struct Graph {
    nodes: Arc<DashSet<Uid>>,
    edges: Arc<DashSet<(Uid, Uid)>>,

    type_index: Arc<DashMap<Uid, TypeRep>>,
    term_index: Arc<DashMap<Uid, TermRep>>,

    type_to_edge_index: Arc<DashMap<Uid, (Uid, Uid)>>,
    edge_to_type_index: Arc<DashMap<(Uid, Uid), Uid>>,
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
        }
    }

    pub fn add_node(&self, r#type: Type, term: Option<Term>) -> Result<Uid, ImplicaError> {
        let type_uid = self.insert_type(r#type);

        if let Some(term) = term {
            self.insert_term(term);
        }

        self.nodes.insert(type_uid);

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

        self.edges.insert(edge_uid);

        Ok(edge_uid)
    }

    pub fn remove_node(&self, node_uid: &Uid) -> Option<Uid> {
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
                self.remove_edge(&edge);
            }

            Some(uid)
        } else {
            None
        }
    }

    pub fn remove_edge(&self, edge_uid: &(Uid, Uid)) -> Option<(Uid, Uid)> {
        let uid = self.edges.remove(edge_uid)?;
        let (_, type_uid) = self.edge_to_type_index.remove(edge_uid)?;
        self.type_to_edge_index.remove(&type_uid)?;

        Some(uid)
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

    pub fn match_type_schema(
        &self,
        type_schema: &TypeSchema,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        self.match_type_pattern(&type_schema.compiled, matches)
    }

    fn match_type_pattern(
        &self,
        pattern: &TypePattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value();
            let r#match = r#match.clone();

            self.type_index.par_iter().try_for_each(|entry| {
                match self.check_type_matches(entry.key(), pattern, r#match.clone()) {
                    Ok(new_match_op) => {
                        if let Some(new_match) = new_match_op {
                            out_map.insert(next_match_id(), (*entry.key(), new_match));
                        }
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(e),
                }
            })
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }

    fn check_type_matches(
        &self,
        type_uid: &Uid,
        pattern: &TypePattern,
        r#match: Arc<Match>,
    ) -> Result<Option<Arc<Match>>, ImplicaError> {
        if let Some(type_row) = self.type_index.get(type_uid) {
            match pattern {
                TypePattern::Wildcard => Ok(Some(r#match.clone())),
                TypePattern::Variable(var) => {
                    if let Some(ref old_element) = r#match.get(var) {
                        match old_element {
                            MatchElement::Type(old_uid) => {
                                if old_uid == type_row.key() {
                                    Ok(Some(r#match.clone()))
                                } else {
                                    Ok(None)
                                }
                            }
                            MatchElement::Term(_) => Err(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "term".to_string(),
                                new: "type".to_string(),
                                context: Some("check type matches".to_string()),
                            }),
                            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "node".to_string(),
                                new: "type".to_string(),
                                context: Some("check type matches".to_string()),
                            }),
                            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "edge".to_string(),
                                new: "type".to_string(),
                                context: Some("check type matches".to_string()),
                            }),
                        }
                    } else {
                        match type_row.value() {
                            TypeRep::Variable(type_name) => {
                                if var == type_name {
                                    Ok(Some(r#match.clone()))
                                } else {
                                    Ok(None)
                                }
                            }
                            _ => Ok(None),
                        }
                    }
                }
                TypePattern::Arrow { left, right } => match type_row.value() {
                    TypeRep::Arrow(left_uid, right_uid) => {
                        if let Some(left_match) =
                            self.check_type_matches(left_uid, left, r#match.clone())?
                        {
                            self.check_type_matches(right_uid, right, left_match.clone())
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                },
                TypePattern::Capture { name, pattern } => {
                    if let Some(capture_match) =
                        self.check_type_matches(type_uid, pattern, r#match.clone())?
                    {
                        let new_match = Match::new(Some(capture_match));
                        new_match.insert(name, MatchElement::Type(*type_uid))?;

                        Ok(Some(Arc::new(new_match)))
                    } else {
                        Ok(None)
                    }
                }
            }
        } else {
            Err(ImplicaError::TypeNotFound {
                uid: *type_uid,
                context: Some("check type matches".to_string()),
            })
        }
    }

    pub fn match_term_schema(
        &self,
        term_schema: &TermSchema,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        self.match_term_pattern(&term_schema.compiled, matches)
    }

    fn match_term_pattern(
        &self,
        pattern: &TermPattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value();
            let r#match = r#match.clone();

            self.term_index.par_iter().try_for_each(|entry| {
                match self.check_term_matches(entry.key(), pattern, r#match.clone()) {
                    Ok(new_match_op) => {
                        if let Some(new_match) = new_match_op {
                            out_map.insert(next_match_id(), (*entry.key(), new_match));
                        }
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(e),
                }
            })
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }

    fn check_term_matches(
        &self,
        term_uid: &Uid,
        pattern: &TermPattern,
        r#match: Arc<Match>,
    ) -> Result<Option<Arc<Match>>, ImplicaError> {
        if let Some(term_row) = self.term_index.get(term_uid) {
            match pattern {
                TermPattern::Wildcard => Ok(Some(r#match.clone())),
                TermPattern::Variable(var) => {
                    if let Some(ref old_element) = r#match.get(var) {
                        match old_element {
                            MatchElement::Term(old_uid) => {
                                if old_uid == term_uid {
                                    Ok(Some(r#match.clone()))
                                } else {
                                    Ok(None)
                                }
                            }
                            MatchElement::Type(_) => Err(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "type".to_string(),
                                new: "term".to_string(),
                                context: Some("check term matches".to_string()),
                            }),
                            MatchElement::Node(_) => Err(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "node".to_string(),
                                new: "term".to_string(),
                                context: Some("check term matches".to_string()),
                            }),
                            MatchElement::Edge(_) => Err(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "edge".to_string(),
                                new: "term".to_string(),
                                context: Some("check term matches".to_string()),
                            }),
                        }
                    } else {
                        let new_match = Match::new(Some(r#match.clone()));
                        new_match.insert(var, MatchElement::Term(*term_uid))?;

                        Ok(Some(Arc::new(new_match)))
                    }
                }
                TermPattern::Application { function, argument } => match term_row.value() {
                    TermRep::Application(function_uid, argument_uid) => {
                        if let Some(function_match) =
                            self.check_term_matches(function_uid, function, r#match.clone())?
                        {
                            self.check_term_matches(argument_uid, argument, function_match)
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                },
                TermPattern::Constant { .. } => todo!("constants not supported yet"),
            }
        } else {
            Err(ImplicaError::TermNotFound {
                uid: *term_uid,
                context: Some("check term matches".to_string()),
            })
        }
    }

    pub fn match_node_pattern(
        &self,
        pattern: &NodePattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value().clone();

            if let Some(ref var) = pattern.variable {
                if let Some(ref old_element) = r#match.get(var) {
                    match old_element {
                        MatchElement::Node(old) => {
                            let mut new_match = r#match.clone();
                            if let Some(ref type_schema) = pattern.type_schema {
                                let res =
                                    self.check_type_matches(old, &type_schema.compiled, new_match);

                                match res {
                                    Ok(m) => match m {
                                        Some(m) => new_match = m.clone(),
                                        None => return ControlFlow::Continue(()),
                                    },
                                    Err(e) => return ControlFlow::Break(e),
                                }
                            }
                            if let Some(ref term_schema) = pattern.term_schema {
                                let res =
                                    self.check_term_matches(old, &term_schema.compiled, new_match);

                                match res {
                                    Ok(m) => match m {
                                        Some(m) => new_match = m.clone(),
                                        None => return ControlFlow::Continue(()),
                                    },
                                    Err(e) => return ControlFlow::Break(e),
                                }
                            }

                            out_map.insert(next_match_id(), (*old, new_match));

                            return ControlFlow::Continue(());
                        }
                        MatchElement::Type(_) => {
                            return ControlFlow::Break(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "type".to_string(),
                                new: "node".to_string(),
                                context: Some("match node pattern".to_string()),
                            });
                        }
                        MatchElement::Term(_) => {
                            return ControlFlow::Break(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "term".to_string(),
                                new: "node".to_string(),
                                context: Some("match node pattern".to_string()),
                            });
                        }
                        MatchElement::Edge(_) => {
                            return ControlFlow::Break(ImplicaError::ContextConflict {
                                name: var.clone(),
                                original: "edge".to_string(),
                                new: "node".to_string(),
                                context: Some("match node pattern".to_string()),
                            });
                        }
                    }
                }
            }
            let mut match_set = Arc::new(DashMap::new());
            match_set.insert(next_match_id(), (_prev_uid, r#match));

            if let Some(ref type_schema) = pattern.type_schema {
                match_set = match self.match_type_schema(type_schema, match_set) {
                    Ok(m) => m,
                    Err(e) => return ControlFlow::Break(e),
                };
            }
            match_set.par_iter().try_for_each(|entry| {
                let (prev_uid, m) = entry.value().clone();

                if let Some(ref term_schema) = pattern.term_schema {
                    match self.check_term_matches(&prev_uid, &term_schema.compiled, m.clone()) {
                        Ok(m) => match m {
                            Some(m) => {
                                out_map.insert(next_match_id(), (prev_uid, m));
                                ControlFlow::Continue(())
                            }
                            None => ControlFlow::Continue(()),
                        },
                        Err(e) => match e {
                            ImplicaError::TermNotFound { .. } => ControlFlow::Continue(()),
                            _ => ControlFlow::Break(e),
                        },
                    }
                } else {
                    out_map.insert(next_match_id(), (prev_uid, m));
                    ControlFlow::Continue(())
                }
            })
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }
}
