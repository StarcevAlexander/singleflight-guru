use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};

pub struct SingleFlight<K, V, E> {
    inflight: Mutex<HashMap<K, Arc<Entry<V, E>>>>,
}

struct Entry<V, E> {
    inner: Mutex<Inner<V, E>>,
    done: Condvar,
}

enum Inner<V, E> {
    Running,
    Finished(Result<Arc<V>, Arc<E>>),
    Aborted,
}

impl<V, E> Entry<V, E> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::Running),
            done: Condvar::new(),
        }
    }
}

impl<K, V, E> Default for SingleFlight<K, V, E>
where
    K: Eq + std::hash::Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, E> SingleFlight<K, V, E>
where
    K: Eq + std::hash::Hash,
{
    pub fn new() -> Self {
        Self {
            inflight: Mutex::new(HashMap::new()),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    pub fn forget(&self, key: &K) {
        self.inflight.lock().unwrap().remove(key);
    }
}

impl<K, V, E> SingleFlight<K, V, E>
where
    K: Eq + std::hash::Hash + Clone,
    V: Send + Sync,
    E: Send + Sync,
{
    pub fn execute<F>(&self, key: K, f: F) -> Result<Arc<V>, Arc<E>>
    where
        F: FnOnce() -> Result<V, E>,
    {
        loop {
            let (entry, is_leader) = {
                let mut guard = self.inflight.lock().unwrap();
                match guard.get(&key) {
                    Some(existing) => (Arc::clone(existing), false),
                    None => {
                        let fresh = Arc::new(Entry::new());
                        guard.insert(key.clone(), Arc::clone(&fresh));
                        (fresh, true)
                    }
                }
            };
            if is_leader {
                return self.run_leader(&key, &entry, f);
            }
            let outcome = self.wait_on(&key, &entry);
            if let Some(result) = outcome {
                return result;
            }
        }
    }

    fn run_leader<F>(&self, key: &K, entry: &Arc<Entry<V, E>>, f: F) -> Result<Arc<V>, Arc<E>>
    where
        F: FnOnce() -> Result<V, E>,
    {
        let outcome = panic::catch_unwind(AssertUnwindSafe(f));
        let mut inner = entry.inner.lock().unwrap();
        match outcome {
            Ok(Ok(value)) => {
                let shared = Arc::new(value);
                *inner = Inner::Finished(Ok(Arc::clone(&shared)));
                entry.done.notify_all();
                drop(inner);
                self.remove_if_same(key, entry);
                Ok(shared)
            }
            Ok(Err(err)) => {
                let shared = Arc::new(err);
                *inner = Inner::Finished(Err(Arc::clone(&shared)));
                entry.done.notify_all();
                drop(inner);
                self.remove_if_same(key, entry);
                Err(shared)
            }
            Err(payload) => {
                *inner = Inner::Aborted;
                entry.done.notify_all();
                drop(inner);
                self.remove_if_same(key, entry);
                panic::resume_unwind(payload);
            }
        }
    }

    fn wait_on(&self, key: &K, entry: &Arc<Entry<V, E>>) -> Option<Result<Arc<V>, Arc<E>>> {
        let mut inner = entry.inner.lock().unwrap();
        loop {
            match &*inner {
                Inner::Running => inner = entry.done.wait(inner).unwrap(),
                Inner::Finished(result) => return Some(result.clone()),
                Inner::Aborted => {
                    drop(inner);
                    self.remove_if_same(key, entry);
                    return None;
                }
            }
        }
    }

    fn remove_if_same(&self, key: &K, entry: &Arc<Entry<V, E>>) {
        let mut guard = self.inflight.lock().unwrap();
        let same = guard
            .get(key)
            .map(|current| Arc::ptr_eq(current, entry))
            .unwrap_or(false);
        if same {
            guard.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    fn spin_until(flag: &AtomicBool, value: bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while flag.load(Ordering::Relaxed) != value {
            assert!(std::time::Instant::now() < deadline, "timeout in test");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn concurrent_calls_share_one_execution() {
        let sf: Arc<SingleFlight<String, u64, String>> = Arc::new(SingleFlight::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));

        let leader = {
            let sf = Arc::clone(&sf);
            let calls = Arc::clone(&calls);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            thread::spawn(move || {
                sf.execute("key".to_string(), || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    started.store(true, Ordering::Relaxed);
                    while !release.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Ok(42)
                })
            })
        };
        spin_until(&started, true);

        let mut waiters = Vec::new();
        for _ in 0..4 {
            let sf = Arc::clone(&sf);
            let calls = Arc::clone(&calls);
            waiters.push(thread::spawn(move || {
                sf.execute("key".to_string(), || {
                    calls.fetch_add(1000, Ordering::Relaxed);
                    Ok(0)
                })
            }));
        }

        assert_eq!(sf.in_flight(), 1);
        release.store(true, Ordering::Relaxed);
        assert_eq!(leader.join().unwrap().unwrap().as_ref(), &42);
        for waiter in waiters {
            assert_eq!(waiter.join().unwrap().unwrap().as_ref(), &42);
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(sf.in_flight(), 0);
    }

    #[test]
    fn different_keys_execute_independently() {
        let sf: SingleFlight<String, u64, String> = SingleFlight::new();
        let a = sf.execute("a".to_string(), || Ok(1)).unwrap();
        let b = sf.execute("b".to_string(), || Ok(2)).unwrap();
        assert_eq!(a.as_ref(), &1);
        assert_eq!(b.as_ref(), &2);
    }

    #[test]
    fn errors_are_shared_too() {
        let sf: Arc<SingleFlight<String, u64, String>> = Arc::new(SingleFlight::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));

        let leader = {
            let sf = Arc::clone(&sf);
            let calls = Arc::clone(&calls);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            thread::spawn(move || {
                sf.execute("key".to_string(), || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    started.store(true, Ordering::Relaxed);
                    while !release.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(String::from("db down"))
                })
            })
        };
        spin_until(&started, true);

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let sf = Arc::clone(&sf);
            waiters.push(thread::spawn(move || {
                sf.execute("key".to_string(), || Ok(0))
            }));
        }

        release.store(true, Ordering::Relaxed);
        assert_eq!(leader.join().unwrap().unwrap_err().as_ref(), "db down");
        for waiter in waiters {
            assert_eq!(waiter.join().unwrap().unwrap_err().as_ref(), "db down");
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn forget_forces_reexecution() {
        let sf: SingleFlight<String, u64, String> = SingleFlight::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let first = sf
            .execute("key".to_string(), || {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(7)
            })
            .unwrap();
        assert_eq!(first.as_ref(), &7);
        sf.forget(&"key".to_string());
        let second = sf
            .execute("key".to_string(), || {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(7)
            })
            .unwrap();
        assert_eq!(second.as_ref(), &7);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn panic_aborts_flight_and_waiter_retries() {
        let sf: Arc<SingleFlight<String, u64, String>> = Arc::new(SingleFlight::new());
        let started = Arc::new(AtomicBool::new(false));

        let leader = {
            let sf = Arc::clone(&sf);
            let started = Arc::clone(&started);
            thread::spawn(move || {
                sf.execute("key".to_string(), || {
                    started.store(true, Ordering::Relaxed);
                    panic!("boom");
                })
            })
        };
        spin_until(&started, true);
        thread::sleep(Duration::from_millis(20));

        let retried = sf.execute("key".to_string(), || Ok(41)).unwrap();
        assert_eq!(retried.as_ref(), &41);
        assert!(leader.join().is_err());
    }

    #[test]
    fn sequential_calls_reexecute() {
        let sf: SingleFlight<String, u64, String> = SingleFlight::new();
        let first = sf.execute("k".to_string(), || Ok(1)).unwrap();
        let second = sf.execute("k".to_string(), || Ok(2)).unwrap();
        assert_eq!(first.as_ref(), &1);
        assert_eq!(second.as_ref(), &2);
    }
}
