use parking_lot::{Condvar, Mutex};

#[derive(Debug)]
struct CreationCounterState {
    limit: u32,
    in_flight: usize,
}

#[derive(Debug)]
pub(crate) struct CreationCounter {
    state: Mutex<CreationCounterState>,
    condvar: Condvar,
}

impl CreationCounter {
    pub(crate) fn new(limit: u32) -> Self {
        Self {
            state: Mutex::new(CreationCounterState {
                limit,
                in_flight: 0,
            }),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn is_available(&self) -> bool {
        let state = self.state.lock();

        state.limit + (state.in_flight as u32) > 0
    }

    pub(crate) fn acquire(&self) -> Option<SlotGuard<'_>> {
        let mut state = self.state.lock();

        loop {
            if state.limit > 0 {
                state.limit -= 1;
                state.in_flight += 1;
                return Some(SlotGuard {
                    counter: self,
                    committed: false,
                });
            } else if state.in_flight == 0 {
                return None;
            } else {
                self.condvar.wait(&mut state);
            }
        }
    }

    fn rollback(&self) {
        let mut state = self.state.lock();
        state.limit += 1;
        state.in_flight -= 1;
        self.condvar.notify_all();
    }

    fn commit(&self) {
        let mut state = self.state.lock();
        state.in_flight -= 1;
        self.condvar.notify_all();
    }
}

pub(crate) struct SlotGuard<'a> {
    counter: &'a CreationCounter,
    committed: bool,
}

impl<'a> SlotGuard<'a> {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl<'a> Drop for SlotGuard<'a> {
    fn drop(&mut self) {
        if self.committed {
            self.counter.commit();
        } else {
            self.counter.rollback();
        }
    }
}
