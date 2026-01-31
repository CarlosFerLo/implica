use pyo3::prelude::*;
use rayon::iter::IntoParallelRefIterator;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use rayon::prelude::*;

use crate::errors::ImplicaError;
use crate::matches::Match;
use crate::patterns::{TermPattern, TermSchema, TypePattern, TypeSchema};
use crate::properties::PropertyMap;
use crate::query::Query;
use crate::typing::{Application, Arrow, BasicTerm, Term, Type, Variable};

#[path = "matches/edge.rs"]
mod __matches_edge_pattern;
#[path = "matches/node.rs"]
mod __matches_node_pattern;
#[path = "matches/path.rs"]
mod __matches_path_pattern;
#[path = "matches/properties.rs"]
mod __matches_properties;
#[path = "matches/term_schema.rs"]
mod __matches_term_schema;
#[path = "matches/type_schema.rs"]
mod __matches_type_schema;

#[path = "create.rs"]
mod __create;

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
type EdgeSet = Arc<DashSet<(Uid, Uid)>>;

#[derive(Clone, Debug)]
pub struct Graph {
    nodes: Arc<DashMap<Uid, PropertyMap>>,
    edges: Arc<DashMap<(Uid, Uid), PropertyMap>>,

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
    pub(crate) fn new() -> Self {
        Graph {
            nodes: Arc::new(DashMap::new()),
            edges: Arc::new(DashMap::new()),
            type_index: Arc::new(DashMap::new()),
            term_index: Arc::new(DashMap::new()),
            type_to_edge_index: Arc::new(DashMap::new()),
            edge_to_type_index: Arc::new(DashMap::new()),
            start_to_edge_index: Arc::new(DashMap::new()),
            end_to_edge_index: Arc::new(DashMap::new()),
        }
    }

    pub(in crate::graph) fn add_node(
        &self,
        r#type: Type,
        term: Option<Term>,
        properties: PropertyMap,
    ) -> Result<Uid, ImplicaError> {
        let type_uid = self.insert_type(&r#type);

        if let Some(term) = term {
            self.insert_term(&term);
        }

        self.nodes.insert(type_uid, properties);
        self.start_to_edge_index
            .insert(type_uid, Arc::new(DashSet::new()));
        self.end_to_edge_index
            .insert(type_uid, Arc::new(DashSet::new()));

        Ok(type_uid)
    }

    pub(in crate::graph) fn add_edge(
        &self,
        term: Term,
        properties: PropertyMap,
    ) -> Result<(Uid, Uid), ImplicaError> {
        let term_uid = self.insert_term(&term);

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

        self.edges.insert(edge_uid, properties);

        Ok(edge_uid)
    }

    pub(crate) fn remove_node(&self, node_uid: &Uid) -> Result<Option<Uid>, ImplicaError> {
        if let Some((uid, _)) = self.nodes.remove(node_uid) {
            let start_by_node: Vec<(Uid, Uid)> = match self.start_to_edge_index.get(&uid) {
                Some(l) => l.value().clone(),
                None => Arc::new(DashSet::new()),
            }
            .par_iter()
            .map(|e| *e.key())
            .collect();
            let ends_by_node: Vec<(Uid, Uid)> = match self.end_to_edge_index.get(&uid) {
                Some(l) => l.value().clone(),
                None => Arc::new(DashSet::new()),
            }
            .par_iter()
            .map(|e| *e.key())
            .collect();

            let edges_to_remove: Vec<(Uid, Uid)> =
                start_by_node.into_iter().chain(ends_by_node).collect();
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

    pub(crate) fn remove_edge(
        &self,
        edge_uid: &(Uid, Uid),
    ) -> Result<Option<(Uid, Uid)>, ImplicaError> {
        let (uid, _) = match self.edges.remove(edge_uid) {
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

    pub(in crate::graph) fn insert_type(&self, r#type: &Type) -> Uid {
        match r#type {
            Type::Variable(var) => {
                let type_rep = TypeRep::Variable(var.name.clone());
                let type_uid = type_rep.uid();

                self.type_index.insert(type_uid, type_rep);
                type_uid
            }
            Type::Arrow(arr) => {
                let left_uid = self.insert_type(arr.left.as_ref());
                let right_uid = self.insert_type(arr.right.as_ref());

                let type_rep = TypeRep::Arrow(left_uid, right_uid);
                let type_uid = type_rep.uid();

                self.type_index.insert(type_uid, type_rep);

                type_uid
            }
        }
    }

    pub(in crate::graph) fn insert_term(&self, term: &Term) -> Uid {
        let term_type = term.r#type();
        let type_uid = self.insert_type(term_type.as_ref());

        match term {
            Term::Basic(term) => {
                let term_rep = TermRep::Base(term.name.clone());

                self.term_index.insert(type_uid, term_rep);
            }
            Term::Application(app) => {
                let function_uid = self.insert_term(app.function.as_ref());
                let argument_uid = self.insert_term(app.argument.as_ref());

                let term_rep = TermRep::Application(function_uid, argument_uid);
                self.term_index.insert(type_uid, term_rep);
            }
        }

        type_uid
    }
}

impl Graph {
    pub(in crate::graph) fn type_schema_to_type(
        &self,
        type_schema: &TypeSchema,
        r#match: Arc<Match>,
    ) -> Result<Type, ImplicaError> {
        self.pattern_to_type_recursive(&type_schema.compiled, r#match)
            .map_err(|e| match e {
                ImplicaError::InvalidPattern { pattern: _, reason } => {
                    ImplicaError::InvalidPattern {
                        pattern: type_schema.pattern.clone(),
                        reason,
                    }
                }
                _ => e,
            })
    }

    fn pattern_to_type_recursive(
        &self,
        pattern: &TypePattern,
        r#match: Arc<Match>,
    ) -> Result<Type, ImplicaError> {
        match pattern {
            TypePattern::Wildcard => Err(ImplicaError::InvalidPattern {
                pattern: "".to_string(),
                reason: "Cannot convert wildcard to type".to_string(),
            }),
            TypePattern::Arrow { left, right } => {
                let left_type = self.pattern_to_type_recursive(left, r#match.clone())?;
                let right_type = self.pattern_to_type_recursive(right, r#match.clone())?;

                Ok(Type::Arrow(Arrow {
                    left: Arc::new(left_type),
                    right: Arc::new(right_type),
                }))
            }
            TypePattern::Variable(var) => {
                if let Some(match_element) = r#match.get(var) {
                    let matched_type_uid = match_element
                        .as_type(var, Some("pattern to type recursive".to_string()))?;

                    self.type_from_uid(&matched_type_uid)
                } else {
                    Ok(Type::Variable(Variable::new(var.clone())?))
                }
            }
            TypePattern::Capture { name, pattern: _ } => {
                if let Some(match_element) = r#match.get(name) {
                    let matched_type_uid = match_element
                        .as_type(name, Some("pattern to type recursive".to_string()))?;

                    self.type_from_uid(&matched_type_uid)
                } else {
                    Ok(Type::Variable(Variable::new(name.clone())?))
                }
            }
        }
    }

    fn type_from_uid(&self, uid: &Uid) -> Result<Type, ImplicaError> {
        if let Some(entry) = self.type_index.get(uid) {
            let type_repr = entry.value().clone();

            match type_repr {
                TypeRep::Variable(var) => Ok(Type::Variable(Variable::new(var)?)),
                TypeRep::Arrow(left, right) => {
                    let left_type =
                        self.type_from_uid(&left)
                            .map_err(|_| ImplicaError::IndexCorruption {
                                message:
                                    "type repr points to a uid that does not belong to the index!"
                                        .to_string(),
                                context: Some("type from uid".to_string()),
                            })?;
                    let right_type =
                        self.type_from_uid(&right)
                            .map_err(|_| ImplicaError::IndexCorruption {
                                message:
                                    "type repr points to a uid that does not belong to the index!"
                                        .to_string(),
                                context: Some("type from uid".to_string()),
                            })?;

                    Ok(Type::Arrow(Arrow {
                        left: Arc::new(left_type),
                        right: Arc::new(right_type),
                    }))
                }
            }
        } else {
            Err(ImplicaError::TypeNotFound {
                uid: *uid,
                context: Some("type from uid".to_string()),
            })
        }
    }
}

impl Graph {
    pub(in crate::graph) fn term_schema_to_term(
        &self,
        term_schema: &TermSchema,
        r#match: Arc<Match>,
    ) -> Result<Term, ImplicaError> {
        self.pattern_to_term_recursive(&term_schema.compiled, r#match)
            .map_err(|e| match e {
                ImplicaError::InvalidPattern { pattern: _, reason } => {
                    ImplicaError::InvalidPattern {
                        pattern: term_schema.pattern.clone(),
                        reason,
                    }
                }
                _ => e,
            })
    }

    fn pattern_to_term_recursive(
        &self,
        pattern: &TermPattern,
        r#match: Arc<Match>,
    ) -> Result<Term, ImplicaError> {
        match pattern {
            TermPattern::Wildcard => Err(ImplicaError::InvalidPattern {
                pattern: "".to_string(),
                reason: "Cannot convert wildcard to term".to_string(),
            }),
            TermPattern::Application { function, argument } => {
                let function_term = self.pattern_to_term_recursive(function, r#match.clone())?;
                let argument_term = self.pattern_to_term_recursive(argument, r#match.clone())?;

                Ok(Term::Application(Application::new(
                    function_term,
                    argument_term,
                )?))
            }
            TermPattern::Variable(var) => {
                if let Some(match_element) = r#match.get(var) {
                    let term_uid = match_element
                        .as_term(var, Some("pattern to term recursive".to_string()))?;

                    self.term_from_uid(&term_uid)
                } else {
                    Err(ImplicaError::VariableNotFound {
                        name: var.clone(),
                        context: Some("pattern to term recursive".to_string()),
                    })
                }
            }
            TermPattern::Constant { name: _, args: _ } => {
                todo!("Constants are not implemented yet!")
            }
        }
    }

    fn term_from_uid(&self, uid: &Uid) -> Result<Term, ImplicaError> {
        if let Some(entry) = self.term_index.get(uid) {
            let term_repr = entry.value().clone();

            let term_type = self.type_from_uid(uid).map_err(|e| {
                match e {
                    ImplicaError::TypeNotFound { .. } => ImplicaError::IndexCorruption { message: "Found a term in the TermIndex without its corresponding type in the TypeIndex".to_string(), context: Some("term from uid".to_string()) },
                    _ => e
                }
            })?;

            match term_repr {
                TermRep::Base(var) => Ok(Term::Basic(BasicTerm::new(
                    var.clone(),
                    Arc::new(term_type),
                )?)),
                TermRep::Application(left, right) => {
                    let left_term = self.term_from_uid(&left)?;
                    let right_term = self.term_from_uid(&right)?;

                    Ok(Term::Application(Application::new(left_term, right_term)?))
                }
            }
        } else {
            Err(ImplicaError::TermNotFound {
                uid: *uid,
                context: Some("term from uid".to_string()),
            })
        }
    }
}

impl Graph {
    pub(crate) fn type_to_string(&self, r#type: &Uid) -> Result<String, ImplicaError> {
        if let Some(entry) = self.type_index.get(r#type) {
            let type_rep = entry.value();

            match type_rep {
                TypeRep::Variable(var) => Ok(var.clone()),
                TypeRep::Arrow(left, right) => Ok(format!(
                    "({} -> {})",
                    self.type_to_string(left)?,
                    self.type_to_string(right)?
                )),
            }
        } else {
            Err(ImplicaError::TypeNotFound {
                uid: *r#type,
                context: Some("type to string".to_string()),
            })
        }
    }

    pub(crate) fn term_to_string(&self, term: &Uid) -> Result<String, ImplicaError> {
        if let Some(entry) = self.term_index.get(term) {
            let term_rep = entry.value();

            match term_rep {
                TermRep::Base(var) => Ok(var.clone()),
                TermRep::Application(func, arg) => Ok(format!(
                    "({} {})",
                    self.term_to_string(func)?,
                    self.term_to_string(arg)?
                )),
            }
        } else {
            Err(ImplicaError::TermNotFound {
                uid: *term,
                context: Some("term to string".to_string()),
            })
        }
    }

    pub(crate) fn node_to_string(&self, node: &Uid) -> Result<String, ImplicaError> {
        if let Some(entry) = self.nodes.get(node) {
            let props = entry.value();

            Ok(format!(
                "Node({}:{}:{})",
                self.type_to_string(node)?,
                self.term_to_string(node).unwrap_or_else(|_| "".to_string()),
                props
            ))
        } else {
            Err(ImplicaError::NodeNotFound {
                uid: *node,
                context: Some("edge to string".to_string()),
            })
        }
    }

    pub(crate) fn edge_to_string(&self, edge: &(Uid, Uid)) -> Result<String, ImplicaError> {
        if let Some(entry) = self.edges.get(edge) {
            let props = entry.value();

            let edge_type = match self.edge_to_type_index.get(edge) {
                Some(t) => *t.value(),
                None => return Err(ImplicaError::IndexCorruption { message: "missing entry of edge that appears in the EdgeIndex but not in the EdgeToTypeIndex".to_string(), context: Some("edge to string".to_string()) })
            };

            Ok(format!(
                "Edge({}:{}:{})",
                self.type_to_string(&edge_type)?,
                self.term_to_string(&edge_type)?,
                props
            )
            .to_string())
        } else {
            Err(ImplicaError::EdgeNotFound {
                uid: *edge,
                context: Some("edge to string".to_string()),
            })
        }
    }
}

impl Graph {
    pub(crate) fn node_properties(&self, node: &Uid) -> Result<PropertyMap, ImplicaError> {
        if let Some(entry) = self.nodes.get(node) {
            Ok(entry.value().clone())
        } else {
            Err(ImplicaError::NodeNotFound {
                uid: *node,
                context: Some("node properties".to_string()),
            })
        }
    }

    pub(crate) fn edge_properties(&self, edge: &(Uid, Uid)) -> Result<PropertyMap, ImplicaError> {
        if let Some(entry) = self.edges.get(edge) {
            Ok(entry.value().clone())
        } else {
            Err(ImplicaError::EdgeNotFound {
                uid: *edge,
                context: Some("edge properties".to_string()),
            })
        }
    }
}

impl Graph {
    pub(crate) fn set_node_properties(&self, node: &Uid, properties: PropertyMap) {
        self.nodes.insert(*node, properties);
    }

    pub(crate) fn set_edge_properties(&self, edge: &(Uid, Uid), properties: PropertyMap) {
        self.edges.insert(*edge, properties);
    }
}

#[pyclass(name = "Graph")]
#[derive(Debug, Clone)]
pub struct PyGraph {
    graph: Arc<Graph>,
}

impl Default for PyGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl PyGraph {
    #[new]
    pub fn new() -> Self {
        let graph = Graph::new();

        PyGraph {
            graph: Arc::new(graph),
        }
    }

    pub fn query(&self) -> Query {
        Query::new(self.graph.clone())
    }
}
