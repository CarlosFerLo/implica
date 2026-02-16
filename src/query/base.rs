use std::collections::HashMap;
use std::fmt::Display;
use std::ops::ControlFlow;
use std::sync::Arc;

use error_stack::{Report, ResultExt};
use pyo3::prelude::*;
use pyo3::types::PyList;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::ctx;
use crate::errors::{ImplicaResult, IntoPyResult};
use crate::matches::{default_match_set, MatchElement};
use crate::properties::PropertyMap;
use crate::query::references::*;
use crate::{errors::ImplicaError, graph::Graph, matches::MatchSet, patterns::PathPattern};

#[derive(Debug, Clone)]
enum QueryOperation {
    Create(PathPattern),
    Match(PathPattern),
    Remove(Vec<String>),
    Set(String, PropertyMap, bool),
}

impl Display for QueryOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryOperation::Create(pattern) => write!(f, "CREATE {}", pattern),
            QueryOperation::Match(pattern) => write!(f, "MATCH {}", pattern),
            QueryOperation::Remove(variables) => {
                write!(f, "REMOVE ")?;
                let mut is_first = true;

                for var in variables.iter() {
                    if !is_first {
                        write!(f, ", ")?;
                    }
                    is_first = false;
                    write!(f, "{}", var)?;
                }

                Ok(())
            }
            QueryOperation::Set(variable, properties, overwrite) => {
                write!(
                    f,
                    "SET {} {} {}",
                    variable,
                    if *overwrite { "=" } else { "+=" },
                    properties
                )
            }
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Query {
    graph: Arc<Graph>,
    operations: Vec<QueryOperation>,
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for op in self.operations.iter() {
            writeln!(f, "{}", op)?;
        }

        Ok(())
    }
}

impl Query {
    pub(crate) fn new(graph: Arc<Graph>) -> Self {
        Query {
            graph,
            operations: Vec::new(),
        }
    }

    fn execute_operations(&self) -> ImplicaResult<MatchSet> {
        let mut mset: MatchSet = default_match_set();

        for op in self.operations.iter() {
            match op {
                QueryOperation::Create(pattern) => {
                    mset = self.execute_create(pattern, mset).attach(ctx!(format!(
                        "query - execute operation - {}",
                        self.to_string()
                    )))?;
                }
                QueryOperation::Match(pattern) => {
                    mset = self.execute_match(pattern, mset).attach(ctx!(format!(
                        "query - execute operation - {}",
                        self.to_string()
                    )))?;
                }
                QueryOperation::Remove(variables) => {
                    mset = self.execute_remove(variables, mset).attach(ctx!(format!(
                        "query - execute operation - {}",
                        self.to_string()
                    )))?;
                }
                QueryOperation::Set(variable, properties, overwrite) => {
                    mset = self
                        .execute_set(variable, properties, *overwrite, mset)
                        .attach(ctx!(format!(
                            "query - execute operation - {}",
                            self.to_string()
                        )))?;
                }
            }
        }

        Ok(mset)
    }

    fn execute_create(&self, pattern: &PathPattern, matches: MatchSet) -> ImplicaResult<MatchSet> {
        self.graph
            .create_path(pattern, matches)
            .attach(ctx!(format!("query - execute create - {}", pattern)))
    }

    fn execute_match(&self, pattern: &PathPattern, matches: MatchSet) -> ImplicaResult<MatchSet> {
        self.graph
            .match_path_pattern(pattern, matches)
            .attach(ctx!(format!("query - execute match - {}", pattern)))
    }

    fn execute_remove(&self, variables: &[String], matches: MatchSet) -> ImplicaResult<MatchSet> {
        for var in variables.iter() {
            let result = matches.par_iter().try_for_each(|entry| {
                let (_, r#match) = entry.value().clone();

                if let Some(element) = r#match.remove(var) {
                    match element {
                        MatchElement::Node(n) => match self.graph.remove_node(&n) {
                            Ok(_) => ControlFlow::Continue(()),
                            Err(e) => ControlFlow::Break(e),
                        },
                        MatchElement::Edge(e) => match self.graph.remove_edge(&e) {
                            Ok(_) => ControlFlow::Continue(()),
                            Err(e) => ControlFlow::Break(e),
                        },
                        MatchElement::Term(_) => ControlFlow::Break(
                            ImplicaError::InvalidQuery {
                                query: self.to_string(),
                                reason: "You cannot remove terms from the graph".to_string(),
                                context: Some("execute remove".to_string()),
                            }
                            .into(),
                        ),
                        MatchElement::Type(_) => ControlFlow::Break(
                            ImplicaError::InvalidQuery {
                                query: self.to_string(),
                                reason: "You cannot remove types from the graph".to_string(),
                                context: Some("execute remove".to_string()),
                            }
                            .into(),
                        ),
                    }
                } else {
                    ControlFlow::Break(
                        ImplicaError::VariableNotFound {
                            name: var.clone(),
                            context: Some("execute remove".to_string()),
                        }
                        .into(),
                    )
                }
            });

            match result {
                ControlFlow::Continue(()) => (),
                ControlFlow::Break(e) => {
                    return Err(e.attach(ctx!(format!(
                        "query - execute remove - {}",
                        variables.join(", ")
                    ))))
                }
            }
        }

        Ok(matches)
    }

    fn execute_set(
        &self,
        variable: &str,
        properties: &PropertyMap,
        overwrite: bool,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        let result: ControlFlow<Report<ImplicaError>> = matches.par_iter().try_for_each(|entry| {
            let (_, r#match) = entry.value().clone();

            if let Some(element) = r#match.get(variable) {
                match element {
                    MatchElement::Node(n) => {
                        match self.graph.set_node_properties(&n, properties.clone(), overwrite) {
                            Ok(()) => ControlFlow::Continue(()),
                            Err(e) => ControlFlow::Break(e.attach(ctx!("query - execute set")))
                        }

                    }
                    MatchElement::Edge(e) => {
                        match self.graph.set_edge_properties(&e, properties.clone(), overwrite) {
                            Ok(()) => ControlFlow::Continue(()),
                            Err(e) => ControlFlow::Break(e.attach(ctx!("query - execute set")))
                        }
                    }
                    MatchElement::Type(_) => ControlFlow::Break(ImplicaError::InvalidQuery {
                        query: self.to_string(),
                        reason:
                            "You cannot set the properties of a type, types do not have properties"
                                .to_string(),
                        context: Some("execute set".to_string()),
                    }.into()),
                    MatchElement::Term(_) => ControlFlow::Break(ImplicaError::InvalidQuery {
                        query: self.to_string(),
                        reason:
                            "You cannot set the properties of a type, types do not have properties"
                                .to_string(),
                        context: Some("execute set".to_string()),
                    }.into()),
                }
            } else {
                ControlFlow::Break(
                    ImplicaError::VariableNotFound {
                        name: variable.to_string(),
                        context: Some("execute set".to_string()),
                    }
                    .into(),
                )
            }
        });

        match result {
            ControlFlow::Continue(()) => Ok(matches),
            ControlFlow::Break(e) => Err(e.attach(ctx!(format!(
                "query - execute set - {} {} {}",
                variable,
                if overwrite { "=" } else { "+=" },
                properties
            )))),
        }
    }
}

#[pymethods]
impl Query {
    pub fn create(&mut self, pattern: String) -> PyResult<Query> {
        let path_pattern = PathPattern::new(pattern)
            .attach(ctx!("query - create"))
            .into_py_result()?;

        self.operations.push(QueryOperation::Create(path_pattern));

        Ok(self.clone())
    }

    pub fn r#match(&mut self, pattern: String) -> PyResult<Query> {
        let path_pattern = PathPattern::new(pattern)
            .attach(ctx!("query - match"))
            .into_py_result()?;
        self.operations.push(QueryOperation::Match(path_pattern));
        Ok(self.clone())
    }

    #[pyo3(signature=(*variables))]
    pub fn remove(&mut self, variables: Vec<String>) -> Query {
        self.operations.push(QueryOperation::Remove(variables));
        self.clone()
    }

    #[pyo3(signature = (variable, properties, overwrite=true))]
    pub fn set(
        &mut self,
        variable: String,
        properties: &Bound<PyAny>,
        overwrite: bool,
    ) -> PyResult<Query> {
        let map = PropertyMap::new(properties)
            .attach(ctx!("query - set"))
            .into_py_result()?;

        self.operations
            .push(QueryOperation::Set(variable, map, overwrite));
        Ok(self.clone())
    }

    pub fn execute(&mut self) -> PyResult<()> {
        self.execute_operations()
            .attach(ctx!("query - execute"))
            .into_py_result()?;
        Ok(())
    }

    #[pyo3(signature=(*variables))]
    pub fn return_<'py>(
        &mut self,
        py: Python<'py>,
        variables: Vec<String>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mset = self
            .execute_operations()
            .attach(ctx!("query - return"))
            .into_py_result()?;

        let results: Vec<HashMap<String, Reference>> = mset
            .par_iter()
            .map(|entry| {
                let (_prev_uid, r#match) = entry.value().clone();

                let mut map = HashMap::new();

                for v in variables.iter() {
                    if let Some(element) = r#match.get(v) {
                        let reference = match element {
                            MatchElement::Edge(uid) => {
                                Reference::Edge(EdgeRef::new(self.graph.clone(), uid))
                            }
                            MatchElement::Node(uid) => {
                                Reference::Node(NodeRef::new(self.graph.clone(), uid))
                            }
                            MatchElement::Term(uid) => {
                                Reference::Term(TermRef::new(self.graph.clone(), uid))
                            }
                            MatchElement::Type(uid) => {
                                Reference::Type(TypeRef::new(self.graph.clone(), uid))
                            }
                        };

                        map.insert(v.clone(), reference);
                    } else {
                        return Err(ImplicaError::VariableNotFound {
                            name: v.clone(),
                            context: Some(ctx!("query return - data collection").to_string()),
                        }
                        .into());
                    }
                }

                Ok(map)
            })
            .collect::<ImplicaResult<Vec<_>>>()
            .into_py_result()?;

        let py_results = PyList::empty(py);

        for map in results {
            py_results.append(map.into_pyobject(py)?)?; // TODO: attach something here
        }

        Ok(py_results)
    }

    pub fn __str__(&self) -> String {
        self.to_string()
    }
}
