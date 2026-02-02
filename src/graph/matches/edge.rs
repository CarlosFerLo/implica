use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use error_stack::Report;
use rayon::prelude::*;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::Graph;
use crate::graph::Uid;
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::{CompiledDirection, EdgePattern};

impl Graph {
    pub(super) fn match_edge_pattern(
        &self,
        pattern: &EdgePattern,
        start: Option<String>,
        end: Option<String>,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let (start, end) = match pattern.compiled_direction {
            CompiledDirection::Forward => (start, end),
            CompiledDirection::Backward => (end, start),
            CompiledDirection::Any => todo!("Any Direction not supported yet!"),
        };

        let result = matches
            .par_iter()
            .try_for_each(|entry| -> ControlFlow<Report<ImplicaError>> {
                let (_prev_uid, r#match) = entry.value().clone();

                // Check if match already holds the desired edge
                if let Some(ref var) = pattern.variable {
                    if let Some(old) = r#match.get(var) {
                        let old_edge =
                            match old.as_edge(var, Some("match edge pattern".to_string())) {
                                Ok(e) => e,
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            };

                        let old_edge_type = match self.edge_to_type_index.get(&old_edge) {
                            Some(uid) => *uid.value(),
                            None => {
                                return ControlFlow::Break(ImplicaError::IndexCorruption {
                                    message: "missing type for edge in edge_to_type_index"
                                        .to_string(),
                                    context: Some("match edge pattern".to_string()),
                                }.into())
                            }
                        };

                        let mut new_match = Arc::new(Match::new(Some(r#match.clone())));

                        // Check if its endpoints satisfy the check
                        if let Some(cf) =
                            Self::match_endpoints(start.clone(), end.clone(), old_edge, new_match.clone())
                        {
                            return cf.map_break(|e| e.attach(ctx!("graph - match edge pattern")));
                        }

                        // Check if its type satisfies the type schema
                        if let Some(ref type_schema) = pattern.type_schema {
                            new_match = match self.check_type_matches(
                                &old_edge_type,
                                &type_schema.compiled,
                                new_match,
                            ) {
                                Ok(m) => match m {
                                    Some(m) => m,
                                    None => return ControlFlow::Continue(()),
                                },
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            }
                        }

                        // Check if its term satisfies the term schema
                        if let Some(ref term_schema) = pattern.term_schema {
                            new_match = match self.check_term_matches(
                                &old_edge_type,
                                &term_schema.compiled,
                                new_match,
                            ) {
                                Ok(m) => match m {
                                    Some(m) => m,
                                    None => return ControlFlow::Continue(()),
                                },
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            }
                        }

                        // Check if properties match
                        if let Some(ref properties) = pattern.properties {
                            match self
                                .check_edge_matches_properties(&old_edge, properties)
                            {
                                Ok(true) => (),
                                Ok(false) => return ControlFlow::Continue(()),
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            }
                        }

                        out_map.insert(next_match_id(), (old_edge_type, new_match));

                        return ControlFlow::Continue(());
                    }
                }

                // Filter by type schema if provided
                if let Some(ref type_schema) = pattern.type_schema {

                    let matches: MatchSet = Arc::new(DashMap::from_iter([(
                        next_match_id(),
                        (_prev_uid, r#match),
                    )]));

                    let possible_types = match self.match_type_schema(type_schema, matches.clone())
                    {
                        Ok(m) => m,
                        Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                    };

                    return possible_types.par_iter().try_for_each(|entry| {
                        let (edge_type, r#match) = entry.value().clone();

                        let mut new_match = Arc::new(Match::new(Some(r#match)));

                        // Check if there is an edge of that type
                        let edge = match self.type_to_edge_index.get(&edge_type) {
                            Some(e) => *e.value(),
                            None => return ControlFlow::Continue(()),
                        };

                        // Check if its endpoints satisfy the check
                        if let Some(cf) = Self::match_endpoints(
                            start.clone(),
                            end.clone(),
                            edge,
                            new_match.clone(),
                        ) {
                            return cf.map_break(|e| e.attach(ctx!("graph - match edge pattern")));
                        }

                        // Check if its term satisfies the term schema
                        if let Some(ref term_schema) = pattern.term_schema {
                            new_match = match self.check_term_matches(
                                &edge_type,
                                &term_schema.compiled,
                                new_match,
                            ) {
                                Ok(m) => match m {
                                    Some(m) => m,
                                    None => return ControlFlow::Continue(()),
                                },
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            }
                        }

                        // Check if its properties match
                        if let Some(ref properties) = pattern.properties {
                            match self
                                .check_edge_matches_properties(&edge, properties)
                            {
                                Ok(true) => (),
                                Ok(false) => return ControlFlow::Continue(()),
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            }
                        }

                        // Insert edge to the match if var is specified
                        if let Some(ref var) = pattern.variable {
                            if let Err(e) = new_match.insert(var, MatchElement::Edge(edge)) {
                                return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                            }
                        }

                        out_map.insert(next_match_id(), (edge_type, new_match));

                        ControlFlow::Continue(())
                    });
                }

                // Filter by term schema if provided
                if let Some(ref term_schema) = pattern.term_schema {
                    let matches: MatchSet = Arc::new(DashMap::from_iter([(
                    next_match_id(),
                    (_prev_uid, r#match),
                )]));

                    let possible_terms = match self.match_term_schema(term_schema, matches) {
                        Ok(m) => m,
                        Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                    };

                    return possible_terms.par_iter().try_for_each(|entry| {
                        let (edge_type, r#match) = entry.value().clone();

                        let new_match = Arc::new(Match::new(Some(r#match)));

                        // Check if there is an edge of that type
                        let edge = match self.type_to_edge_index.get(&edge_type) {
                            Some(e) => *e.value(),
                            None => return ControlFlow::Continue(()),
                        };

                        // Check if its endpoints satisfy the check
                        if let Some(cf) = Self::match_endpoints(
                            start.clone(),
                            end.clone(),
                            edge,
                            new_match.clone(),
                        ) {
                            return cf;
                        }

                        // Check if its properties match
                        if let Some(ref properties) = pattern.properties {
                            match self
                                .check_edge_matches_properties(&edge, properties)
                            {
                                Ok(true) => (),
                                Ok(false) => return ControlFlow::Continue(()),
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                            }
                        }

                        // Insert edge to the match if var is specified
                        if let Some(ref var) = pattern.variable {
                            if let Err(e) = new_match.insert(var, MatchElement::Edge(edge)) {
                                return  ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                            }
                        }

                        out_map.insert(next_match_id(), (edge_type, new_match));

                        ControlFlow::Continue(())
                    });
                }

                // Filter by endpoints as default
                let start_node = if let Some(ref start) = start {
                    match self.get_node_uid(start, r#match.clone()) {
                        Ok(n) => n,
                        Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                    }
                } else {
                    None
                };

                let end_node = if let Some(ref start) = start {
                    match self.get_node_uid(start, r#match.clone()) {
                        Ok(n) => n,
                        Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                    }
                } else {
                    None
                };

                match (start_node, end_node) {
                    (Some(start_node), Some(end_node)) => {
                        let new_match = Arc::new(Match::new(Some(r#match.clone())));

                        let edge_type = match self.edge_to_type_index.get(&(start_node, end_node)) {
                            Some(res) => *res.value(),
                            None => return ControlFlow::Continue(()),
                        };

                        if let Some(ref properties) = pattern.properties {
                                        match self
                                            .check_edge_matches_properties(&(start_node, end_node), properties)
                                        {
                                            Ok(true) => (),
                                            Ok(false) => return ControlFlow::Continue(()),
                                            Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                                        }
                                    }

                        if let Some(ref var) = pattern.variable {
                            if let Err(e) = new_match.insert(var, MatchElement::Edge((start_node, end_node))) {
                                return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                            }
                        }

                        out_map.insert(next_match_id(), (edge_type, new_match.clone()));

                        ControlFlow::Continue(())
                    }
                    (Some(start_node), None) => {
                        let possible_edges = match self.start_to_edge_index.get(&start_node) {
                            Some(m) => m.value().clone(),
                            None => return ControlFlow::Continue(()),
                        };

                        possible_edges.par_iter().try_for_each(|edge| {
                            let edge = *edge.key();

                            if let Some(ref properties) = pattern.properties {
                                match self
                                    .check_edge_matches_properties(&edge, properties)
                                {
                                    Ok(true) => (),
                                    Ok(false) => return ControlFlow::Continue(()),
                                    Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                                }
                            }

                            let new_match = Arc::new(Match::new(Some(r#match.clone())));

                            if let Some(ref end) = end {
                                if let Err(e) = new_match.insert(end, MatchElement::Node(edge.1)) {
                                    return  ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }

                            if let Some(ref var) = pattern.variable {
                                if let Err(e) = new_match.insert(var, MatchElement::Edge(edge)) {
                                    return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }

                            let edge_type =
                                match self.edge_to_type_index.get(&edge) {
                                    Some(res) => *res.value(),
                                    None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "edge belongs to some key in start_to_edge_index but does not appear in edge_to_type_index".to_string(), context: Some("match edge pattern".to_string()) }.into()),
                                };

                            out_map.insert(next_match_id(), (edge_type, new_match));

                            ControlFlow::Continue(())
                        })
                    }

                    (None, Some(end_node)) => {
                        let possible_edges = match self.end_to_edge_index.get(&end_node) {
                            Some(m) => m.value().clone(),
                            None => return ControlFlow::Continue(())
                        };

                        possible_edges.par_iter().try_for_each(|edge| {
                            let edge = *edge.key();

                            if let Some(ref properties) = pattern.properties {
                                match self
                                    .check_edge_matches_properties(&edge, properties)
                                {
                                    Ok(true) => (),
                                    Ok(false) => return ControlFlow::Continue(()),
                                    Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                                }
                            }

                            let new_match = Arc::new(Match::new(Some(r#match.clone())));

                            if let Some(ref start) = start {
                                if let Err(e) = new_match.insert(start, MatchElement::Node(edge.0)) {
                                    return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }

                            if let Some(ref var) = pattern.variable {
                                if let Err(e) = new_match.insert(var, MatchElement::Edge(edge)) {
                                    return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }

                            let edge_type =
                                match self.edge_to_type_index.get(&edge) {
                                    Some(res) => *res.value(),
                                    None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "edge belongs to some key in end_to_edge_index but does not appear in edge_to_type_index".to_string(), context: Some("match edge pattern".to_string()) }.into()),
                                };

                            out_map.insert(next_match_id(), (edge_type, new_match));

                            ControlFlow::Continue(())
                        })
                    }
                    (None, None) => {
                        self.edges.par_iter().try_for_each(|edge| {
                            let edge = *edge.key();

                            if let Some(ref properties) = pattern.properties {
                                match self
                                    .check_edge_matches_properties(&edge, properties)
                                {
                                    Ok(true) => (),
                                    Ok(false) => return ControlFlow::Continue(()),
                                    Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern"))),
                                }
                            }

                            let new_match = Arc::new(Match::new(Some(r#match.clone())));

                            if let Some(ref start) = start {
                                if let Err(e) = new_match.insert(start, MatchElement::Node(edge.0)) {
                                    return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }
                            if let Some(ref end) = end {
                                if let Err(e) = new_match.insert(end, MatchElement::Node(edge.1)) {
                                    return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }
                            if let Some(ref var) = pattern.variable {
                                if let Err(e) = new_match.insert(var, MatchElement::Edge(edge)) {
                                    return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")));
                                }
                            }

                            let edge_type =
                                match self.edge_to_type_index.get(&edge) {
                                    Some(res) => *res.value(),
                                    None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "edge belongs to some key in end_to_edge_index but does not appear in edge_to_type_index".to_string(), context: Some("match edge pattern".to_string()) }.into()),
                                };

                            out_map.insert(next_match_id(), (edge_type, new_match));
                            ControlFlow::Continue(())
                        })
                    }
                }
            });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }

    fn match_endpoints(
        start: Option<String>,
        end: Option<String>,
        edge: (Uid, Uid),
        r#match: Arc<Match>,
    ) -> Option<ControlFlow<Report<ImplicaError>>> {
        if let Some(ref start) = start {
            if let Some(start_element) = r#match.get(start) {
                let start_node =
                    match start_element.as_node(start, Some("match edge pattern".to_string())) {
                        Ok(n) => n,
                        Err(e) => {
                            return Some(ControlFlow::Break(
                                e.attach(ctx!("graph - match endpoints")),
                            ))
                        }
                    };

                if start_node != edge.0 {
                    return Some(ControlFlow::Continue(()));
                }
            } else {
                match r#match.insert(start, MatchElement::Node(edge.0)) {
                    Ok(()) => (),
                    Err(e) => {
                        return Some(ControlFlow::Break(
                            e.attach(ctx!("graph - match endpoints")),
                        ))
                    }
                }
            }
        }

        if let Some(ref end) = end {
            if let Some(end_element) = r#match.get(end) {
                let end_node =
                    match end_element.as_node(end, Some("match edge pattern".to_string())) {
                        Ok(n) => n,
                        Err(e) => {
                            return Some(ControlFlow::Break(
                                e.attach(ctx!("graph - match endpoints")),
                            ))
                        }
                    };

                if end_node != edge.1 {
                    return Some(ControlFlow::Continue(()));
                }
            } else {
                match r#match.insert(end, MatchElement::Node(edge.1)) {
                    Ok(()) => (),
                    Err(e) => {
                        return Some(ControlFlow::Break(
                            e.attach(ctx!("graph - match endpoints")),
                        ))
                    }
                }
            }
        }

        None
    }

    fn get_node_uid(&self, var: &str, r#match: Arc<Match>) -> ImplicaResult<Option<Uid>> {
        match r#match.get(var) {
            Some(n) => match n.as_node(var, Some("match edge".to_string())) {
                Ok(n) => Ok(Some(n)),
                Err(e) => Err(e.attach(ctx!("graph - get node uid"))),
            },
            None => Ok(None),
        }
    }
}
