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

    pub fn prev(&self) -> impl Iterator<Item = String> {
        (0..self.counter.load(Ordering::SeqCst)).map(Self::format)
    }
}
