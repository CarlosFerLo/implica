use parking_lot::deadlock;
use std::{thread, time::Duration};

pub(crate) fn start_deadlock_watchdog() {
    thread::Builder::new()
        .name("deadlock-watchdog".into())
        .spawn(|| loop {
            thread::sleep(Duration::from_secs(5));

            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            eprintln!("🔴 {} DEADLOCK(S) DETECTED", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                eprintln!("  Deadlock #{}", i + 1);
                for t in threads {
                    eprintln!("    Thread ID {:?}", t.thread_id());
                    eprintln!("    Backtrace:\n{:#?}", t.backtrace());
                }
            }

            panic!("deadlock detected – aborting");
        })
        .expect("watchdog thread spawn failed");
}
