//! Type indexing structures for fast O(log n) type matching.
//!
//! This module provides tree-based indexing structures that allow fast lookup
//! of types based on their Application structure. Instead of iterating through
//! all types (O(n)), we can use the tree structure to perform lookups in O(log n).

use crate::types::Type;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A tree-based index for types that enables O(log n) lookups.
///
/// The TypeIndex organizes types by their structure:
/// - Variable types are stored in a simple HashMap by name
/// - Application types are stored in a tree structure indexed by their left and right types
///
/// This allows matching algorithms to quickly find candidate types without
/// iterating through all types in the system.
#[derive(Clone, Debug, Default)]
pub struct TypeIndex<T: Clone> {
    /// Index for Variable types: maps variable name to items
    variable_index: HashMap<String, Vec<T>>,

    /// Index for Application types: nested structure
    /// First level: left type UID -> Second level: right type UID -> items
    application_index: BTreeMap<String, BTreeMap<String, Vec<T>>>,

    /// Wildcard index: stores all items for wildcard matching (matches any type)
    all_items: Vec<T>,
}

impl<T: Clone> TypeIndex<T> {
    /// Creates a new empty type index.
    pub fn new() -> Self {
        TypeIndex {
            variable_index: HashMap::new(),
            application_index: BTreeMap::new(),
            all_items: Vec::new(),
        }
    }

    /// Inserts an item indexed by a type.
    ///
    /// The item will be indexed according to the structure of the type,
    /// allowing for fast lookups later.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type to index by
    /// * `item` - The item to store
    pub fn insert(&mut self, typ: &Type, item: T) {
        // Always add to all_items for wildcard matching
        self.all_items.push(item.clone());

        match typ {
            Type::Variable(var) => {
                // Index by variable name
                self.variable_index
                    .entry(var.name.clone())
                    .or_default()
                    .push(item);
            }
            Type::Application(app) => {
                // Index by left and right type UIDs
                let left_uid = app.left.uid();
                let right_uid = app.right.uid();

                self.application_index
                    .entry(left_uid)
                    .or_default()
                    .entry(right_uid)
                    .or_default()
                    .push(item);
            }
        }
    }

    /// Removes an item from the index.
    ///
    /// This is more expensive than insertion as it requires searching through
    /// the stored items. Use a filter function to identify which item to remove.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type the item was indexed under
    /// * `filter` - A function that returns true for the item to remove
    pub fn remove<F>(&mut self, typ: &Type, filter: F)
    where
        F: Fn(&T) -> bool,
    {
        // Remove from all_items
        self.all_items.retain(|item| !filter(item));

        match typ {
            Type::Variable(var) => {
                if let Some(items) = self.variable_index.get_mut(&var.name) {
                    items.retain(|item| !filter(item));
                    if items.is_empty() {
                        self.variable_index.remove(&var.name);
                    }
                }
            }
            Type::Application(app) => {
                let left_uid = app.left.uid();
                let right_uid = app.right.uid();

                if let Some(right_map) = self.application_index.get_mut(&left_uid) {
                    if let Some(items) = right_map.get_mut(&right_uid) {
                        items.retain(|item| !filter(item));
                        if items.is_empty() {
                            right_map.remove(&right_uid);
                        }
                    }
                    if right_map.is_empty() {
                        self.application_index.remove(&left_uid);
                    }
                }
            }
        }
    }

    /// Finds items that match a specific variable type.
    ///
    /// # Arguments
    ///
    /// * `var_name` - The name of the variable to match
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_variable(&self, var_name: &str) -> Vec<&T> {
        self.variable_index
            .get(var_name)
            .map(|items| items.iter().collect())
            .unwrap_or_default()
    }

    /// Finds items that match a specific application type.
    ///
    /// # Arguments
    ///
    /// * `left_type` - The left type of the application
    /// * `right_type` - The right type of the application
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_application(&self, left_type: &Type, right_type: &Type) -> Vec<&T> {
        let left_uid = left_type.uid();
        let right_uid = right_type.uid();

        self.application_index
            .get(&left_uid)
            .and_then(|right_map| right_map.get(&right_uid))
            .map(|items| items.iter().collect())
            .unwrap_or_default()
    }

    /// Finds items where the application's left type matches.
    ///
    /// This is useful for partial matching when we only care about the left side
    /// of an application type.
    ///
    /// # Arguments
    ///
    /// * `left_type` - The left type to match
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_application_by_left(&self, left_type: &Type) -> Vec<&T> {
        let left_uid = left_type.uid();

        self.application_index
            .get(&left_uid)
            .map(|right_map| right_map.values().flat_map(|items| items.iter()).collect())
            .unwrap_or_default()
    }

    /// Finds items where the application's right type matches.
    ///
    /// This is useful for partial matching when we only care about the right side
    /// of an application type.
    ///
    /// # Arguments
    ///
    /// * `right_type` - The right type to match
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_application_by_right(&self, right_type: &Type) -> Vec<&T> {
        let right_uid = right_type.uid();

        self.application_index
            .values()
            .flat_map(|right_map| {
                right_map
                    .get(&right_uid)
                    .map(|items| items.iter())
                    .into_iter()
                    .flatten()
            })
            .collect()
    }

    /// Finds all items (for wildcard matching).
    ///
    /// # Returns
    ///
    /// A vector of references to all items in the index
    pub fn find_all(&self) -> Vec<&T> {
        self.all_items.iter().collect()
    }

    /// Returns the number of items in the index.
    pub fn len(&self) -> usize {
        self.all_items.len()
    }

    /// Returns whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.all_items.is_empty()
    }

    /// Clears all items from the index.
    pub fn clear(&mut self) {
        self.variable_index.clear();
        self.application_index.clear();
        self.all_items.clear();
    }
}

/// A specialized index for types that also stores the actual Type objects.
///
/// This is useful when we need to retrieve both the type and the associated item.
#[derive(Clone, Debug)]
pub struct TypeWithItemIndex<T: Clone> {
    inner: TypeIndex<(Arc<Type>, T)>,
}

impl<T: Clone> TypeWithItemIndex<T> {
    pub fn new() -> Self {
        TypeWithItemIndex {
            inner: TypeIndex::new(),
        }
    }

    pub fn insert(&mut self, typ: Arc<Type>, item: T) {
        self.inner.insert(&typ, (typ.clone(), item));
    }

    pub fn remove<F>(&mut self, typ: &Type, filter: F)
    where
        F: Fn(&T) -> bool,
    {
        self.inner.remove(typ, |(_, item)| filter(item));
    }

    pub fn find_variable(&self, var_name: &str) -> Vec<(&Type, &T)> {
        self.inner
            .find_variable(var_name)
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn find_application(&self, left_type: &Type, right_type: &Type) -> Vec<(&Type, &T)> {
        self.inner
            .find_application(left_type, right_type)
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn find_application_by_left(&self, left_type: &Type) -> Vec<(&Type, &T)> {
        self.inner
            .find_application_by_left(left_type)
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn find_application_by_right(&self, right_type: &Type) -> Vec<(&Type, &T)> {
        self.inner
            .find_application_by_right(right_type)
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn find_all(&self) -> Vec<(&Type, &T)> {
        self.inner
            .find_all()
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<T: Clone> Default for TypeWithItemIndex<T> {
    fn default() -> Self {
        Self::new()
    }
}
