//! Type indexing structures for fast O(log n) type matching.
//!
//! This module provides tree-based indexing structures that allow fast lookup
//! of types based on their Arrow structure. Instead of iterating through
//! all types (O(n)), we can use the tree structure to perform lookups in O(log n).
//!
//! For very large graphs (>100K types), optional Bloom Filters can be enabled
//! to provide O(1) pre-filtering before more expensive operations.

use crate::errors::ImplicaError;
use crate::types::Type;
use pyo3::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Configuration for type index optimization features.
///
/// This allows fine-tuning the indexing strategy based on expected graph size.
/// Bloom filters provide O(1) pre-filtering for very large graphs (>100K types).
///
/// # Python Examples
///
/// ```python
/// import implica
///
/// # Small graph: bloom filters disabled (default)
/// config = implica.IndexConfig()
/// graph = implica.Graph(config)
///
/// # Large graph: enable bloom filters with 1% false positive rate
/// config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1_000_000)
/// graph = implica.Graph(config)
///
/// # Auto-configure for graph size
/// config = implica.IndexConfig.for_graph_size(500_000)
/// graph = implica.Graph(config)
/// ```
///
/// # Rust Examples
///
/// ```rust
/// // Disable bloom filters (default for small graphs)
/// let config = IndexConfig::default();
///
/// // Enable bloom filters with 1% false positive rate (good for large graphs)
/// let config = IndexConfig::new().with_bloom_filter(0.01);
///
/// // Enable bloom filters with 5% false positive rate (uses less memory)
/// let config = IndexConfig::new().with_bloom_filter(0.05);
/// ```
#[pyclass]
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// False positive rate for bloom filters.
    /// - `None`: Bloom filters disabled (default for <100K types)
    /// - `Some(0.001)`: 0.1% false positive rate (high memory, very accurate)
    /// - `Some(0.01)`: 1% false positive rate (recommended for 100K-1M types)
    /// - `Some(0.05)`: 5% false positive rate (recommended for >1M types)
    #[pyo3(get, set)]
    pub bloom_filter_fpr: Option<f64>,

    /// Estimated number of types (used to size bloom filters optimally)
    #[pyo3(get, set)]
    pub estimated_size: Option<usize>,
}

#[pymethods]
impl IndexConfig {
    /// Creates a new config with bloom filters disabled by default.
    ///
    /// # Arguments
    ///
    /// * `bloom_filter_fpr` - Optional false positive rate (0.0 to 1.0)
    /// * `estimated_size` - Optional estimated number of types
    ///
    /// # Python Examples
    ///
    /// ```python
    /// # Bloom filters disabled (default)
    /// config = implica.IndexConfig()
    ///
    /// # Enable bloom filters with 1% FPR
    /// config = implica.IndexConfig(bloom_filter_fpr=0.01)
    ///
    /// # Enable with size hint for optimization
    /// config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1_000_000)
    /// ```
    #[new]
    #[pyo3(signature = (bloom_filter_fpr=None, estimated_size=None))]
    pub fn py_new(bloom_filter_fpr: Option<f64>, estimated_size: Option<usize>) -> PyResult<Self> {
        if let Some(fpr) = bloom_filter_fpr {
            if fpr <= 0.0 || fpr >= 1.0 {
                return Err(ImplicaError::invalid_configuration(
                    "bloom_filter_fpr",
                    format!("{}", fpr),
                    "must be in range (0.0, 1.0)",
                )
                .into());
            }
        }

        Ok(IndexConfig {
            bloom_filter_fpr,
            estimated_size,
        })
    }

    /// Returns a recommended config for the given graph size.
    ///
    /// Automatically selects optimal bloom filter settings based on expected size.
    ///
    /// # Arguments
    ///
    /// * `num_types` - Expected number of types in the graph
    ///
    /// # Returns
    ///
    /// An optimally configured IndexConfig
    ///
    /// # Python Examples
    ///
    /// ```python
    /// # Auto-configure for 500K types
    /// config = implica.IndexConfig.for_graph_size(500_000)
    /// graph = implica.Graph(config)
    ///
    /// # Small graph (no bloom filters)
    /// config = implica.IndexConfig.for_graph_size(5_000)
    ///
    /// # Very large graph (5% FPR for memory efficiency)
    /// config = implica.IndexConfig.for_graph_size(10_000_000)
    /// ```
    #[staticmethod]
    pub fn for_graph_size(num_types: usize) -> Self {
        match num_types {
            0..=10_000 => {
                // Small graphs: no bloom filter needed
                IndexConfig {
                    bloom_filter_fpr: None,
                    estimated_size: None,
                }
            }
            10_001..=100_000 => {
                // Medium graphs: optional, low FPR
                IndexConfig {
                    bloom_filter_fpr: Some(0.001),
                    estimated_size: Some(num_types),
                }
            }
            100_001..=1_000_000 => {
                // Large graphs: bloom filter with 1% FPR
                IndexConfig {
                    bloom_filter_fpr: Some(0.01),
                    estimated_size: Some(num_types),
                }
            }
            _ => {
                // Very large graphs: bloom filter with 5% FPR for memory efficiency
                IndexConfig {
                    bloom_filter_fpr: Some(0.05),
                    estimated_size: Some(num_types),
                }
            }
        }
    }

    /// Checks if bloom filters are enabled.
    ///
    /// # Returns
    ///
    /// `True` if bloom filters are enabled, `False` otherwise
    pub fn has_bloom_filters(&self) -> bool {
        self.bloom_filter_fpr.is_some()
    }

    /// Returns a string representation of the config.
    fn __str__(&self) -> String {
        match (self.bloom_filter_fpr, self.estimated_size) {
            (None, None) => "IndexConfig(bloom_filters=disabled)".to_string(),
            (Some(fpr), None) => format!("IndexConfig(bloom_fpr={:.1}%)", fpr * 100.0),
            (Some(fpr), Some(size)) => format!(
                "IndexConfig(bloom_fpr={:.1}%, estimated_size={})",
                fpr * 100.0,
                size
            ),
            (None, Some(size)) => {
                format!(
                    "IndexConfig(bloom_filters=disabled, estimated_size={})",
                    size
                )
            }
        }
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl IndexConfig {
    /// Creates a new config with bloom filters disabled (Rust-only method).
    pub fn new() -> Self {
        IndexConfig {
            bloom_filter_fpr: None,
            estimated_size: None,
        }
    }

    /// Enables bloom filters with the specified false positive rate (Rust-only method).
    ///
    /// # Arguments
    ///
    /// * `fpr` - False positive rate (0.0 to 1.0, typically 0.001 to 0.05)
    ///
    /// # Panics
    ///
    /// Panics if fpr is not in range (0.0, 1.0)
    pub fn with_bloom_filter(mut self, fpr: f64) -> Self {
        assert!(
            fpr > 0.0 && fpr < 1.0,
            "False positive rate must be in (0.0, 1.0)"
        );
        self.bloom_filter_fpr = Some(fpr);
        self
    }

    /// Sets the estimated number of types for optimal bloom filter sizing (Rust-only method).
    pub fn with_estimated_size(mut self, size: usize) -> Self {
        self.estimated_size = Some(size);
        self
    }
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// A space-efficient probabilistic data structure for membership testing.
///
/// Bloom filters can answer "is X in the set?" with:
/// - Definite NO (100% accurate)
/// - Probable YES (may have false positives)
///
/// This provides O(1) lookups with ~10 bits per element vs ~64 bits for HashSet.
#[derive(Clone, Debug)]
struct BloomFilter {
    /// Bit array (packed as Vec<u64> for efficiency)
    bits: Vec<u64>,
    /// Number of hash functions (k)
    num_hashes: usize,
    /// Total number of bits (m)
    size: usize,
}

impl BloomFilter {
    /// Creates an optimally-sized bloom filter for n elements with false positive rate p.
    ///
    /// Uses the formulas:
    /// - m = -n * ln(p) / (ln(2)^2)  -- optimal bit array size
    /// - k = m/n * ln(2)              -- optimal number of hash functions
    fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        if expected_elements == 0 {
            return BloomFilter {
                bits: vec![],
                num_hashes: 1,
                size: 0,
            };
        }

        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let m =
            (-(expected_elements as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let k = ((m as f64 / expected_elements as f64) * std::f64::consts::LN_2).ceil() as usize;

        let num_words = (m + 63) / 64; // Round up to nearest 64-bit word

        BloomFilter {
            bits: vec![0u64; num_words],
            num_hashes: k.max(1),
            size: m,
        }
    }

    /// Inserts an item into the bloom filter.
    fn insert(&mut self, item: &str) {
        for i in 0..self.num_hashes {
            let index = self.hash(item, i) % self.size;
            let word_index = index / 64;
            let bit_index = index % 64;
            self.bits[word_index] |= 1u64 << bit_index;
        }
    }

    /// Checks if an item might be in the set.
    ///
    /// Returns:
    /// - `false`: Item is definitely NOT in the set
    /// - `true`: Item is probably in the set (may be false positive)
    fn might_contain(&self, item: &str) -> bool {
        for i in 0..self.num_hashes {
            let index = self.hash(item, i) % self.size;
            let word_index = index / 64;
            let bit_index = index % 64;

            if word_index >= self.bits.len() {
                return false;
            }

            if (self.bits[word_index] & (1u64 << bit_index)) == 0 {
                return false; // Definitely not present
            }
        }
        true // Probably present
    }

    /// Generates hash values using double hashing technique.
    fn hash(&self, item: &str, seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize).wrapping_add(seed.wrapping_mul(seed))
    }
}

/// Bloom filters for different type components.
///
/// Maintains separate bloom filters for variables and Arrows
/// to enable fast pre-filtering of type queries.
#[derive(Clone, Debug)]
struct TypeBloomFilters {
    /// Bloom filter for variable names
    /// Answers: "Does any type contain Variable X?"
    variables: BloomFilter,

    /// Bloom filter for Arrow pairs
    /// Answers: "Does any type contain Arrow (L, R)?"
    arrows: BloomFilter,
}

impl TypeBloomFilters {
    fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        // Estimate: each type might have ~5 variable components and ~10 Arrow components
        let estimated_vars = expected_elements * 5;
        let estimated_apps = expected_elements * 10;

        TypeBloomFilters {
            variables: BloomFilter::new(estimated_vars, false_positive_rate),
            arrows: BloomFilter::new(estimated_apps, false_positive_rate),
        }
    }

    fn insert_variable(&mut self, var_name: &str) {
        self.variables.insert(var_name);
    }

    fn insert_arrow(&mut self, left_uid: &str, right_uid: &str) {
        let key = format!("{}|{}", left_uid, right_uid);
        self.arrows.insert(&key);
    }

    fn might_contain_variable(&self, var_name: &str) -> bool {
        self.variables.might_contain(var_name)
    }

    fn might_contain_arrow(&self, left_uid: &str, right_uid: &str) -> bool {
        let key = format!("{}|{}", left_uid, right_uid);
        self.arrows.might_contain(&key)
    }
}

/// Component-based index for precise lookups after bloom filtering.
///
/// Maps type components (variables and Arrows) to the indices
/// of items that contain them, enabling fast intersection queries.
#[derive(Clone, Debug)]
struct ComponentIndex<T: Clone> {
    /// Maps variable name to item indices that contain it
    contains_var: HashMap<String, HashSet<usize>>,

    /// Maps (left_uid, right_uid) to item indices that contain this Arrow
    contains_app: HashMap<(String, String), HashSet<usize>>,

    /// All items stored by index
    items: Vec<T>,
}

impl<T: Clone> ComponentIndex<T> {
    fn new() -> Self {
        ComponentIndex {
            contains_var: HashMap::new(),
            contains_app: HashMap::new(),
            items: Vec::new(),
        }
    }

    fn insert(&mut self, typ: &Type, item: T) -> usize {
        let index = self.items.len();
        self.items.push(item);
        self.index_components(typ, index);
        index
    }

    fn index_components(&mut self, typ: &Type, item_index: usize) {
        match typ {
            Type::Variable(var) => {
                self.contains_var
                    .entry(var.name.clone())
                    .or_default()
                    .insert(item_index);
            }
            Type::Arrow(app) => {
                // Index this Arrow
                let key = (app.left.uid(), app.right.uid());
                self.contains_app.entry(key).or_default().insert(item_index);

                // Recursively index components
                self.index_components(&app.left, item_index);
                self.index_components(&app.right, item_index);
            }
        }
    }

    fn find_with_variable(&self, var_name: &str) -> HashSet<usize> {
        self.contains_var.get(var_name).cloned().unwrap_or_default()
    }

    fn find_with_arrow(&self, left_uid: &str, right_uid: &str) -> HashSet<usize> {
        self.contains_app
            .get(&(left_uid.to_string(), right_uid.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    fn get_item(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }
}

/// A tree-based index for types that enables O(log n) lookups.
///
/// The TypeIndex organizes types by their structure:
/// - Variable types are stored in a simple HashMap by name
/// - Arrow types are stored in a tree structure indexed by their left and right types
///
/// For large graphs, optional Bloom Filters provide O(1) pre-filtering:
/// - When enabled: Bloom → Component Index → Full Match
/// - When disabled: Direct lookup in traditional indices
///
/// This allows matching algorithms to quickly find candidate types without
/// iterating through all types in the system.
///
/// # Examples
///
/// ```rust
/// // Small graph: no bloom filters
/// let index = TypeIndex::<String>::new();
///
/// // Large graph: enable bloom filters
/// let config = IndexConfig::new().with_bloom_filter(0.01);
/// let index = TypeIndex::<String>::with_config(config);
/// ```
#[derive(Clone, Debug)]
pub struct TypeIndex<T: Clone> {
    /// Index for Variable types: maps variable name to items
    variable_index: HashMap<String, Vec<T>>,

    /// Index for Arrow types: nested structure
    /// First level: left type UID -> Second level: right type UID -> items
    arrow_index: BTreeMap<String, BTreeMap<String, Vec<T>>>,

    /// Wildcard index: stores all items for wildcard matching (matches any type)
    all_items: Vec<T>,

    /// Optional Bloom filters for O(1) pre-filtering (None if disabled)
    bloom: Option<TypeBloomFilters>,

    /// Component index for precise multi-component queries
    component_index: ComponentIndex<T>,

    /// Configuration
    config: IndexConfig,
}

impl<T: Clone> TypeIndex<T> {
    /// Creates a new empty type index with default config (bloom filters disabled).
    pub fn new() -> Self {
        Self::with_config(IndexConfig::default())
    }

    /// Creates a new type index with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Index configuration (bloom filter settings, etc.)
    ///
    /// # Examples
    ///
    /// ```rust
    /// // Disable bloom filters (default)
    /// let index = TypeIndex::<String>::new();
    ///
    /// // Enable bloom filters with 1% FPR for 1M expected types
    /// let config = IndexConfig::new()
    ///     .with_bloom_filter(0.01)
    ///     .with_estimated_size(1_000_000);
    /// let index = TypeIndex::<String>::with_config(config);
    ///
    /// // Auto-configure based on graph size
    /// let config = IndexConfig::for_graph_size(500_000);
    /// let index = TypeIndex::<String>::with_config(config);
    /// ```
    pub fn with_config(config: IndexConfig) -> Self {
        let bloom = if let Some(fpr) = config.bloom_filter_fpr {
            let estimated = config.estimated_size.unwrap_or(10_000);
            Some(TypeBloomFilters::new(estimated, fpr))
        } else {
            None
        };

        TypeIndex {
            variable_index: HashMap::new(),
            arrow_index: BTreeMap::new(),
            all_items: Vec::new(),
            bloom,
            component_index: ComponentIndex::new(),
            config,
        }
    }

    /// Returns whether bloom filters are enabled.
    pub fn has_bloom_filters(&self) -> bool {
        self.bloom.is_some()
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &IndexConfig {
        &self.config
    }

    /// Inserts an item indexed by a type.
    ///
    /// The item will be indexed according to the structure of the type,
    /// allowing for fast lookups later. If bloom filters are enabled,
    /// the item is also indexed in the bloom filter and component index.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type to index by
    /// * `item` - The item to store
    pub fn insert(&mut self, typ: &Type, item: T) {
        // Always add to all_items for wildcard matching
        self.all_items.push(item.clone());

        // Traditional indexing
        match typ {
            Type::Variable(var) => {
                // Index by variable name
                self.variable_index
                    .entry(var.name.clone())
                    .or_default()
                    .push(item.clone());
            }
            Type::Arrow(app) => {
                // Index by left and right type UIDs
                let left_uid = app.left.uid();
                let right_uid = app.right.uid();

                self.arrow_index
                    .entry(left_uid)
                    .or_default()
                    .entry(right_uid)
                    .or_default()
                    .push(item.clone());
            }
        }

        // If bloom filters are enabled, index components
        if self.bloom.is_some() {
            self.component_index.insert(typ, item.clone());
            self.insert_into_bloom(typ);
        }
    }

    /// Inserts all components of a type into the bloom filter (recursive).
    fn insert_into_bloom(&mut self, typ: &Type) {
        if let Some(ref mut bloom) = self.bloom {
            match typ {
                Type::Variable(var) => {
                    bloom.insert_variable(&var.name);
                }
                Type::Arrow(app) => {
                    let left_uid = app.left.uid();
                    let right_uid = app.right.uid();
                    bloom.insert_arrow(&left_uid, &right_uid);

                    // Recursively insert components
                    self.insert_into_bloom(&app.left);
                    self.insert_into_bloom(&app.right);
                }
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
            Type::Arrow(app) => {
                let left_uid = app.left.uid();
                let right_uid = app.right.uid();

                if let Some(right_map) = self.arrow_index.get_mut(&left_uid) {
                    if let Some(items) = right_map.get_mut(&right_uid) {
                        items.retain(|item| !filter(item));
                        if items.is_empty() {
                            right_map.remove(&right_uid);
                        }
                    }
                    if right_map.is_empty() {
                        self.arrow_index.remove(&left_uid);
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

    /// Finds items that match a specific Arrow type.
    ///
    /// # Arguments
    ///
    /// * `left_type` - The left type of the Arrow
    /// * `right_type` - The right type of the Arrow
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_arrow(&self, left_type: &Type, right_type: &Type) -> Vec<&T> {
        let left_uid = left_type.uid();
        let right_uid = right_type.uid();

        self.arrow_index
            .get(&left_uid)
            .and_then(|right_map| right_map.get(&right_uid))
            .map(|items| items.iter().collect())
            .unwrap_or_default()
    }

    /// Finds items where the Arrow's left type matches.
    ///
    /// This is useful for partial matching when we only care about the left side
    /// of an Arrow type.
    ///
    /// # Arguments
    ///
    /// * `left_type` - The left type to match
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_arrow_by_left(&self, left_type: &Type) -> Vec<&T> {
        let left_uid = left_type.uid();

        self.arrow_index
            .get(&left_uid)
            .map(|right_map| right_map.values().flat_map(|items| items.iter()).collect())
            .unwrap_or_default()
    }

    /// Finds items where the Arrow's right type matches.
    ///
    /// This is useful for partial matching when we only care about the right side
    /// of an Arrow type.
    ///
    /// # Arguments
    ///
    /// * `right_type` - The right type to match
    ///
    /// # Returns
    ///
    /// A vector of references to matching items
    pub fn find_arrow_by_right(&self, right_type: &Type) -> Vec<&T> {
        let right_uid = right_type.uid();

        self.arrow_index
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

    /// Finds items that contain all specified type components (optimized with bloom filters).
    ///
    /// This method uses a three-phase strategy when bloom filters are enabled:
    /// 1. **Bloom Pre-filtering (O(1))**: Quick rejection if components don't exist
    /// 2. **Component Index Intersection (O(log n))**: Find items with all components
    /// 3. **Full Verification (O(k))**: Verify matches on candidate set (k << n)
    ///
    /// When bloom filters are disabled, falls back to component index search.
    ///
    /// # Arguments
    ///
    /// * `components` - Vector of concrete type components to search for
    ///
    /// # Returns
    ///
    /// A vector of references to items containing all specified components
    ///
    /// # Examples
    ///
    /// ```rust
    /// // Find all types containing both Variable("A") and Variable("B")
    /// let components = vec![
    ///     Type::Variable(Variable { name: "A".to_string() }),
    ///     Type::Variable(Variable { name: "B".to_string() }),
    /// ];
    /// let results = index.find_by_components(&components);
    /// ```
    pub fn find_by_components(&self, components: &[Type]) -> Vec<&T> {
        if components.is_empty() {
            return self.find_all();
        }

        // Phase 1: Bloom pre-filtering (if enabled)
        if let Some(ref bloom) = self.bloom {
            for component in components {
                let might_exist = match component {
                    Type::Variable(var) => bloom.might_contain_variable(&var.name),
                    Type::Arrow(app) => {
                        let left_uid = app.left.uid();
                        let right_uid = app.right.uid();
                        bloom.might_contain_arrow(&left_uid, &right_uid)
                    }
                };

                if !might_exist {
                    // Bloom filter says "definitely NOT present"
                    // Can return empty immediately
                    return vec![];
                }
            }
        }

        // Phase 2: Component index intersection
        let mut candidate_indices: Option<HashSet<usize>> = None;

        for component in components {
            let component_indices = match component {
                Type::Variable(var) => self.component_index.find_with_variable(&var.name),
                Type::Arrow(app) => {
                    let left_uid = app.left.uid();
                    let right_uid = app.right.uid();
                    self.component_index.find_with_arrow(&left_uid, &right_uid)
                }
            };

            candidate_indices = Some(match candidate_indices {
                None => component_indices,
                Some(prev) => prev.intersection(&component_indices).copied().collect(),
            });

            // Early termination if no candidates remain
            if candidate_indices
                .as_ref()
                .map(|set| set.is_empty())
                .unwrap_or(false)
            {
                return vec![];
            }
        }

        // Phase 3: Retrieve items from component index
        if self.bloom.is_some() {
            // Use component index (items are stored there when bloom is enabled)
            candidate_indices
                .unwrap_or_default()
                .iter()
                .filter_map(|&idx| self.component_index.get_item(idx))
                .collect()
        } else {
            // Bloom not enabled, but component index might still have partial data
            // Fall back to traditional lookup
            self.find_all()
        }
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
        self.arrow_index.clear();
        self.all_items.clear();

        // Clear bloom filters and component index if enabled
        if let Some(ref mut bloom) = self.bloom {
            // Recreate empty bloom filters with same config
            if let Some(fpr) = self.config.bloom_filter_fpr {
                let estimated = self.config.estimated_size.unwrap_or(10_000);
                *bloom = TypeBloomFilters::new(estimated, fpr);
            }
        }
        self.component_index = ComponentIndex::new();
    }
}

impl<T: Clone> Default for TypeIndex<T> {
    fn default() -> Self {
        Self::new()
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

    pub fn with_config(config: IndexConfig) -> Self {
        TypeWithItemIndex {
            inner: TypeIndex::with_config(config),
        }
    }

    pub fn has_bloom_filters(&self) -> bool {
        self.inner.has_bloom_filters()
    }

    pub fn config(&self) -> &IndexConfig {
        self.inner.config()
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

    pub fn find_arrow(&self, left_type: &Type, right_type: &Type) -> Vec<(&Type, &T)> {
        self.inner
            .find_arrow(left_type, right_type)
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn find_arrow_by_left(&self, left_type: &Type) -> Vec<(&Type, &T)> {
        self.inner
            .find_arrow_by_left(left_type)
            .into_iter()
            .map(|(typ, item)| (typ.as_ref(), item))
            .collect()
    }

    pub fn find_arrow_by_right(&self, right_type: &Type) -> Vec<(&Type, &T)> {
        self.inner
            .find_arrow_by_right(right_type)
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
