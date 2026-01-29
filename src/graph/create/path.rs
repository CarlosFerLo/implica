use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::errors::ImplicaError;
use crate::graph::base::Graph;
use crate::graph::Uid;
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::{CompiledDirection, PathPattern};
use crate::typing::{Arrow, Term, Type};
use crate::utils::{DataQueue, QueueItem};

struct NodeData {
    variable: Option<String>,
    r#type: Option<Type>,
    term: Option<Term>,
    type_matched: bool,
    term_matched: bool,
}

impl NodeData {
    pub fn new(variable: Option<String>) -> Self {
        NodeData {
            variable,
            r#type: None,
            term: None,
            type_matched: false,
            term_matched: false,
        }
    }
}

struct EdgeData {
    variable: Option<String>,
    direction: CompiledDirection,
    r#type: Option<Type>,
    term: Option<Term>,
    type_matched: bool,
    term_matched: bool,
}

impl EdgeData {
    pub fn new(variable: Option<String>, direction: CompiledDirection) -> Self {
        EdgeData {
            variable,
            direction,
            r#type: None,
            term: None,
            type_matched: false,
            term_matched: false,
        }
    }
}

impl Graph {
    pub fn create_path(
        &self,
        pattern: &PathPattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map = Arc::new(DashMap::new());

        pattern.validate()?;

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value().clone();

            let mut new_match = Arc::new(Match::new(Some(r#match.clone())));

            // -- Initialization of data holders
            let mut nodes_data: Vec<NodeData> = pattern
                .nodes
                .iter()
                .map(|np| {
                    NodeData::new(np.variable.clone())
                })
                .collect();

            let mut edges_data: Vec<EdgeData> = pattern
                .edges
                .iter()
                .map(|ep| {
                    EdgeData::new(ep.variable.clone(), ep.compiled_direction.clone())
                })
                .collect();

            // -- Initialize Queue
            let mut queue= DataQueue::new(nodes_data.len());

            // -- Consume the Queue
            while let Some(item) = queue.pop() {
                if item.is_node {
                    let node_data = match nodes_data.get(item.index) {
                        Some(d) => d,
                        None => {
                            return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                index: item.index,
                                max_len: nodes_data.len(),
                                context: Some("create path - node data inference".to_string()),
                            })
                        }
                    };

                    let mut type_update = None;
                    let mut term_update = None;

                    // -- Populate if already matched
                    if let Some(node_var) = &node_data.variable {
                        if let Some(element) = new_match.get(node_var) {
                            let node = match element.as_node(
                                node_var,
                                Some(
                                    "create path - node data inference - node already matched"
                                        .to_string(),
                                ),
                            ) {
                                Ok(n) => n,
                                Err(e) => return ControlFlow::Break(e),
                            };

                            type_update = match self.type_from_uid(&node) {
                                Ok(t) => Some(t),
                                Err(e) => return ControlFlow::Break(e),
                            };

                            term_update = match self.term_from_uid(&node) {
                                Ok(t) => Some(t),
                                Err(e) => match e {
                                    ImplicaError::TermNotFound { .. } => None,
                                    _ => return ControlFlow::Break(e),
                                },
                            };
                        }
                    }

                    // Update based on patterns

                    let mut type_matched = None;
                    let mut term_matched = None;

                    if !node_data.type_matched || !node_data.term_matched {
                        let node_pattern = match pattern.nodes.get(item.index) {
                            Some(p) => p,
                            None => {
                                return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                    index: item.index,
                                    max_len: pattern.nodes.len(),
                                    context: Some(
                                        "create path - node data inference - match pattern"
                                            .to_string(),
                                    ),
                                })
                            }
                        };

                        if !node_data.type_matched {
                            if let Some(type_schema) = &node_pattern.type_schema {
                                match &node_data.r#type {
                                    Some(t) => {
                                        let type_uid = self.insert_type(t);

                                        match self.check_type_matches(
                                            &type_uid,
                                            &type_schema.compiled,
                                            new_match.clone(),
                                        ) {
                                            Ok(m) => match m {
                                                Some(m) => {
                                                    new_match = m;
                                                    type_matched = Some(true);

                                                    for (i, nd) in nodes_data.iter().enumerate() {
                                                        if !nd.type_matched {
                                                            queue.push(QueueItem::new(i, true));
                                                        }
                                                    }
                                                    for (i, ed) in edges_data.iter().enumerate() {
                                                        if !ed.type_matched {
                                                            queue.push(QueueItem::new(i, false));
                                                        }
                                                    }
                                                }
                                                None => type_matched = Some(false),
                                            },
                                            Err(e) => return ControlFlow::Break(e),
                                        }
                                    }
                                    None => {
                                        type_update = match self
                                            .type_schema_to_type(type_schema, new_match.clone())
                                        {
                                            Ok(t) => Some(t),
                                            Err(e) => return ControlFlow::Break(e),
                                        };
                                        type_matched = Some(true);
                                    }
                                }
                            } else {
                                type_matched = Some(true)
                            }
                        }

                        if !node_data.term_matched {
                            if let Some(term_schema) = &node_pattern.term_schema {
                                match &node_data.term {
                                    Some(t) => {
                                        let term_uid = self.insert_term(t);
                                        match self.check_term_matches(
                                            &term_uid,
                                            &term_schema.compiled,
                                            new_match.clone(),
                                        ) {
                                            Ok(m) => match m {
                                                Some(m) => {
                                                    new_match = m;
                                                    term_matched = Some(true);

                                                    for (i, nd) in nodes_data.iter().enumerate() {
                                                        if !nd.term_matched {
                                                            queue.push(QueueItem::new(i, true));
                                                        }
                                                    }

                                                    for (i, ed) in edges_data.iter().enumerate() {
                                                        if !ed.term_matched {
                                                            queue.push(QueueItem::new(i, false));
                                                        }
                                                    }
                                                }
                                                None => term_matched = Some(false),
                                            },
                                            Err(e) => return ControlFlow::Break(e),
                                        }
                                    }
                                    None => {
                                        term_update = match self
                                            .term_schema_to_term(term_schema, new_match.clone())
                                        {
                                            Ok(t) => {
                                                term_matched = Some(true);
                                                Some(t)
                                            },
                                            Err(e) => match e {
                                                ImplicaError::VariableNotFound { .. } => None,
                                                _ => return ControlFlow::Break(e)
                                            }
                                        };
                                    }
                                }
                            } else {
                                term_matched = Some(true)
                            }
                        }
                    }

                    // Update based on left edge
                    if item.index > 0 {
                        let left_edge_data = match edges_data.get(item.index - 1) {
                            Some(d) => d,
                            None => {
                                return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                    index: item.index - 1,
                                    max_len: edges_data.len(),
                                    context: Some(
                                        "create path - node data inference - left edge".to_string(),
                                    ),
                                })
                            }
                        };

                        if node_data.r#type.is_none() && type_update.is_none() {
                            if let Some(edge_type) = &left_edge_data.r#type {
                                let arrow = match edge_type.as_arrow() {
                                    Some(a) => a,
                                    None => {
                                        return ControlFlow::Break(ImplicaError::InvalidType {
                                            reason: "edge must have an arrow type".to_string(),
                                        })
                                    }
                                };

                                type_update = match left_edge_data.direction {
                                    CompiledDirection::Forward => Some((*arrow.left).clone()),
                                    CompiledDirection::Backward => Some((*arrow.right).clone()),
                                    CompiledDirection::Any => {
                                        todo!("the 'any' direction is not supported yet")
                                    }
                                };
                            }
                        }

                        if node_data.term.is_none() && term_update.is_none() {
                            if let Some(edge_term) = &left_edge_data.term {
                                let left_node_data = match nodes_data.get(item.index - 1) {
                                    Some(d) => d,
                                    None => {
                                        return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                            index: item.index - 1,
                                            max_len: nodes_data.len(),
                                            context: Some(
                                                "create path - node data inference - left node"
                                                    .to_string(),
                                            ),
                                        })
                                    }
                                };

                                if let Some(left_node_term) = &left_node_data.term {
                                    match left_edge_data.direction {
                                        CompiledDirection::Forward => {
                                            term_update = match edge_term.apply(left_node_term) {
                                                Ok(t) => Some(t),
                                                Err(e) => return ControlFlow::Break(e),
                                            }
                                        }
                                        CompiledDirection::Backward => {
                                            if let Some(app) = left_node_term.as_application() {
                                                if *app.function == *edge_term {
                                                    term_update = Some((*app.argument).clone());
                                                }
                                            }
                                        }
                                        CompiledDirection::Any => {
                                            todo!("the 'any' direction is not implemented yet.")
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Update based on right edge
                    if item.index < nodes_data.len() - 1 {
                        let right_edge_data = match edges_data.get(item.index) {
                            Some(d) => d,
                            None => {
                                return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                    index: item.index,
                                    max_len: edges_data.len(),
                                    context: Some(
                                        "create path - node data inference - right edge"
                                            .to_string(),
                                    ),
                                })
                            }
                        };

                        if node_data.r#type.is_none() && type_update.is_none() {
                            if let Some(edge_type) = &right_edge_data.r#type {
                                let arrow = match edge_type.as_arrow() {
                                    Some(a) => a,
                                    None => {
                                        return ControlFlow::Break(ImplicaError::InvalidType {
                                            reason: "edge must have an arrow type".to_string(),
                                        })
                                    }
                                };

                                type_update = match right_edge_data.direction {
                                    CompiledDirection::Forward => Some((*arrow.left).clone()),
                                    CompiledDirection::Backward => Some((*arrow.left).clone()),
                                    CompiledDirection::Any => {
                                        todo!("the 'any' direction is not supported yet.")
                                    }
                                }
                            }
                        }

                        if node_data.term.is_none() && term_update.is_none() {
                            if let Some(edge_term) = &right_edge_data.term {
                                let right_node_data = match nodes_data.get(item.index + 1) {
                                    Some(d) => d,
                                    None => {
                                        return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                            index: item.index - 1,
                                            max_len: nodes_data.len(),
                                            context: Some(
                                                "create path - node data inference - right node"
                                                    .to_string(),
                                            ),
                                        })
                                    }
                                };

                                if let Some(right_node_term) = &right_node_data.term {
                                    match right_edge_data.direction {
                                        CompiledDirection::Forward => {
                                            if let Some(app) = right_node_term.as_application() {
                                                if *app.function == *edge_term {
                                                    term_update = Some((*app.argument).clone());
                                                }
                                            }
                                        }
                                        CompiledDirection::Backward => {
                                            term_update = match edge_term.apply(right_node_term) {
                                                Ok(t) => Some(t),
                                                Err(e) => return ControlFlow::Break(e),
                                            }
                                        }
                                        CompiledDirection::Any => {
                                            todo!("the 'any' direction is not implemented yet.")
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Mutate the node_data

                    if let Some(mut_node_data) = nodes_data.get_mut(item.index) {
                        let mut changed = false;

                        if mut_node_data.r#type.is_none() {
                            mut_node_data.r#type = type_update;
                            changed = true;
                        }
                        if mut_node_data.term.is_none() {
                            mut_node_data.term = term_update;
                            changed = true;
                        }
                        if let Some(m) = type_matched {
                            mut_node_data.type_matched = m;
                        }
                        if let Some(m) = term_matched {
                            mut_node_data.term_matched = m;
                        }

                        if mut_node_data.r#type.is_none() {
                            if let Some(term) = &mut_node_data.term {
                                mut_node_data.r#type = Some((*term.r#type()).clone());

                                changed = true;
                            }
                        }

                        if changed {
                            if item.index > 0 {
                                queue.push(QueueItem::new(item.index - 1, false));
                            }
                            if item.index < nodes_data.len() - 1 {
                                queue.push(QueueItem::new(item.index, false));
                            }
                        }
                    } else {
                        return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                            index: item.index,
                            max_len: nodes_data.len(),
                            context: Some(
                                "create path - node data inference - mutating node data"
                                    .to_string(),
                            ),
                        });
                    }
                } else {
                    // is edge
                    let edge_data = match edges_data.get(item.index) {
                        Some(d) => d,
                        None => {
                            return ControlFlow::Break(ImplicaError::IndexOutOfRange {
                                index: item.index,
                                max_len: edges_data.len(),
                                context: Some("create path - edge data inference".to_string()),
                            })
                        }
                    };

                    let mut type_update = None;
                    let mut term_update = None;

                    // -- Populate if already matched
                    if let Some(edge_var) = &edge_data.variable {
                        if let Some(element) = new_match.get(edge_var) {
                            let edge = match element.as_edge(
                                edge_var,
                                Some(
                                    "create path - edge data inference - edge already matched"
                                        .to_string(),
                                ),
                            ) {
                                Ok(e) => e,
                                Err(e) => return ControlFlow::Break(e),
                            };

                            let edge_type_uid = match self.edge_to_type_index.get(&edge) {
                                Some(t) => *t.value(),
                                None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "Edge exists in EdgeIndex without corresponding entry at EdgeToTypeIndex.".to_string(), context: Some("create path - edge data inference - edge already matched".to_string()) })
                            };

                            type_update = match self.type_from_uid(&edge_type_uid) {
                                Ok(t) => Some(t),
                                Err(e) => return ControlFlow::Break(e)
                            };

                            term_update = match self.term_from_uid(&edge_type_uid) {
                                Ok(t) => Some(t),
                                Err(e) => return ControlFlow::Break(e)
                            };
                        }
                    }

                    // Update based on patterns

                    let mut type_matched = None;
                    let mut term_matched = None;


                    if !edge_data.type_matched || !edge_data.term_matched {
                        let edge_pattern = match pattern.edges.get(item.index) {
                            Some(p) => p,
                            None => {
                                return ControlFlow::Break(ImplicaError::IndexOutOfRange { index: item.index, max_len: pattern.edges.len(), context: Some("create path - edge data inference - match pattern".to_string()) });
                            }
                        };

                        if !edge_data.type_matched {
                            if let Some(type_schema) = &edge_pattern.type_schema {
                                match &edge_data.r#type {
                                    Some(t) => {
                                        let type_uid = self.insert_type(t);

                                        match self.check_type_matches(&type_uid, &type_schema.compiled, new_match.clone()) {
                                            Ok(m) => match m {
                                                Some(m) => {
                                                    new_match = m;
                                                    type_matched = Some(true);

                                                    for (i, nd) in nodes_data.iter().enumerate() {
                                                        if !nd.type_matched {
                                                            queue.push(QueueItem::new(i, true));
                                                        }
                                                    }
                                                    for (i, ed) in edges_data.iter().enumerate() {
                                                        if !ed.type_matched {
                                                            queue.push(QueueItem::new(i, false));
                                                        }
                                                    }
                                                }
                                                None => type_matched = Some(false),
                                            }
                                            Err(e) => return ControlFlow::Break(e)
                                        }
                                    }
                                    None => {
                                        type_update = match self.type_schema_to_type(type_schema, new_match.clone()) {
                                            Ok(t) => Some(t),
                                            Err(e) => return ControlFlow::Break(e)
                                        };
                                        type_matched = Some(true);
                                    }
                                }
                            } else {
                                type_matched = Some(true);
                            }
                        }

                        if !edge_data.term_matched {
                            if let Some(term_schema) = &edge_pattern.term_schema {
                                match &edge_data.term {
                                    Some(t) => {
                                        let term_uid = self.insert_term(t);
                                        match self.check_term_matches(&term_uid, &term_schema.compiled, new_match.clone()){
                                            Ok(m) => match m {
                                                Some(m) => {
                                                    new_match = m;
                                                    term_matched = Some(true);

                                                    for (i, nd) in nodes_data.iter().enumerate() {
                                                        if !nd.term_matched {
                                                            queue.push(QueueItem::new(i, true));
                                                        }
                                                    }
                                                    for (i, ed) in edges_data.iter().enumerate() {
                                                        if !ed.term_matched {
                                                            queue.push(QueueItem::new(i, false));
                                                        }
                                                    }
                                                }
                                                None => term_matched = Some(false)
                                            }
                                            Err(e) => return  ControlFlow::Break(e)
                                        }
                                    }
                                    None => {
                                        term_update = match self.term_schema_to_term(term_schema, new_match.clone()) {
                                            Ok(t) => {
                                                term_matched = Some(true);
                                                Some(t)
                                            },
                                            Err(e) => match e {
                                                ImplicaError::VariableNotFound { .. } => None,
                                                _ => return ControlFlow::Break(e)
                                            }
                                        };

                                    }
                                }
                            } else {
                                term_matched = Some(true);
                            }
                        }
                    }

                    // Update type based on start and end nodes

                    let left_node_data = match nodes_data.get(item.index) {
                        Some(d) => d,
                        None => return ControlFlow::Break(ImplicaError::IndexOutOfRange { index: item.index, max_len: nodes_data.len(), context: Some("create path - edge data inference - left node".to_string()) })
                    };

                    let right_node_data = match nodes_data.get(item.index + 1) {
                        Some(d) => d,
                        None => return ControlFlow::Break(ImplicaError::IndexOutOfRange { index: item.index + 1, max_len: nodes_data.len(), context: Some("create path - edge data inference - right node".to_string()) })
                    };

                    if edge_data.r#type.is_none() && type_update.is_none() {
                        if let Some(left_type) = &left_node_data.r#type {
                            if let Some(right_type) = &right_node_data.r#type {
                                type_update = match edge_data.direction {
                                    CompiledDirection::Forward => Some(Type::Arrow(Arrow::new(Arc::new(left_type.clone()), Arc::new(right_type.clone())))),
                                    CompiledDirection::Backward => Some(Type::Arrow(Arrow::new(Arc::new(right_type.clone()), Arc::new(left_type.clone())))),
                                    CompiledDirection::Any => todo!("the 'any' direction is not supported yet.")
                                }
                            }
                        }
                    }

                    if edge_data.term.is_none() && term_update.is_none() {
                        if let Some(left_term) = &left_node_data.term {
                            if let Some(right_term) = &right_node_data.term {
                                let (left_term, right_term) = match edge_data.direction {
                                    CompiledDirection::Forward => (left_term, right_term),
                                    CompiledDirection::Backward => (right_term, left_term),
                                    CompiledDirection::Any => todo!("the 'any' direction is not supported yet")
                                };

                                if let Some(app) = right_term.as_application() {
                                    if app.argument.as_ref() == left_term {
                                        term_update = Some((*app.function).clone())
                                    }
                                }
                            }
                        }
                    }

                    // Mutate the edge_data

                    if let Some(mut_edge_data) = edges_data.get_mut(item.index) {
                        let mut changed = false;

                        if mut_edge_data.r#type.is_none() {
                            mut_edge_data.r#type = type_update;
                            changed = true;
                        }
                        if mut_edge_data.term.is_none() {
                            mut_edge_data.term = term_update;
                            changed = true;
                        }
                        if let Some(m) = type_matched {
                            mut_edge_data.type_matched = m;
                        }
                        if let Some(m) = term_matched {
                            mut_edge_data.term_matched = m;
                        }

                        if mut_edge_data.r#type.is_none() {
                            if let Some(term) = &mut_edge_data.term {
                                mut_edge_data.r#type = Some((*term.r#type()).clone());

                                changed = true;
                            }
                        }

                        if changed {
                            queue.push(QueueItem::new(item.index, true));
                            queue.push(QueueItem::new(item.index + 1, true));
                        }

                    } else {
                        return ControlFlow::Break(ImplicaError::IndexOutOfRange { index: item.index, max_len: edges_data.len(), context: Some("create path - edge data inference - mutating edge data".to_string()) });
                    }

                }
            }

            // -- Check Inference Succeeded

            for nd in nodes_data.iter() {
                if nd.r#type.is_none() {
                    return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", pattern), reason: "Unable to infer the type of a node contained in the pattern".to_string() })
                }

                if !nd.type_matched {
                    return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", pattern), reason: "Inferred type for node does not match the provided schema".to_string() });
                }

                if nd.term.is_some() && !nd.term_matched {
                    return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", pattern), reason: "Inferred term for node does not match the provided schema".to_string() });
                }
            }

            for ed in edges_data.iter() {
                if ed.term.is_none() {
                    return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", pattern), reason: "Unable to infer the term of an edge contained in the pattern".to_string() });
                }

                if !ed.term_matched {
                    return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", pattern), reason: "Inferred term for edge does not match the provided schema".to_string() });
                }

                if !ed.type_matched {
                   return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", pattern), reason: "Inferred type for edge does not match the provided schema".to_string() });
                }
            }

            // -- Add nodes + edges to the graph

            let mut prev_uid: Uid = [0; 32];

            for nd in nodes_data.into_iter() {
                if let Some(node_var) = &nd.variable {
                    if !new_match.contains_key(node_var) {

                    prev_uid = match self.add_node(nd.r#type.unwrap(), nd.term) {
                        Ok(n) => n,
                        Err(e) => return ControlFlow::Break(e)
                    };

                    match new_match.insert(node_var, MatchElement::Node(prev_uid)) {
                        Ok(()) => (),
                        Err(e) => return ControlFlow::Break(e)
                    }
                }
                } else {
                    match self.add_node(nd.r#type.unwrap(), nd.term) {
                        Ok(..) => (),
                        Err(e) => return ControlFlow::Break(e)
                    }
                }
            }

            for ed in edges_data.into_iter() {
                if let Some(edge_var) = &ed.variable {
                    if !new_match.contains_key(edge_var) {
                    let edge = match self.add_edge(ed.term.unwrap()) {
                        Ok(e) => e,
                        Err(e) => return ControlFlow::Break(e)
                    };

                    match new_match.insert(edge_var, MatchElement::Edge(edge)) {
                        Ok(()) => (),
                        Err(e) => return ControlFlow::Break(e)
                    }
                }
                } else {
                    match self.add_edge(ed.term.unwrap()) {
                        Ok(..) => (),
                        Err(e) => return ControlFlow::Break(e)
                    }
                }

            }

            // -- Add new match to the out map

            out_map.insert(next_match_id(), (prev_uid, new_match));

            ControlFlow::Continue(())
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }
}
