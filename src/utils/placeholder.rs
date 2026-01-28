use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct PlaceholderGenerator {
    counter: AtomicUsize,
}

impl PlaceholderGenerator {
    pub fn new() -> Self {
        PlaceholderGenerator {
            counter: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> String {
        Self::format(self.counter.fetch_add(1, Ordering::SeqCst))
    }

    fn format(n: usize) -> String {
        format!("__ph_{}", n)
    }
}

pub(crate) fn is_placeholder(name: &str) -> bool {
    name.starts_with("__ph_")
}
