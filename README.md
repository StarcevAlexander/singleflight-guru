# singleflight-guru

**Go singleflight-style call deduplication for Rust** — concurrent calls with the same key share one execution and its result, zero dependencies.

For developers coming from Go's [golang.org/x/sync/singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight) who miss `Do(key, fn)` for collapsing duplicate work — cache stampedes, thundering-herd fetches, repeated heavy computations — and for Rust developers who want the same trick without async runtimes or framework locks.

## Usage

```rust
use singleflight_guru::SingleFlight;
use std::sync::Arc;

let sf = Arc::new(SingleFlight::new());

let sf_thread = Arc::clone(&sf);
let leader = std::thread::spawn(move || {
    sf_thread.execute("user:42".to_string(), || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        Ok::<_, String>(String::from("profile data"))
    })
});

let sf_waiter = Arc::clone(&sf);
let waiter = std::thread::spawn(move || {
    sf_waiter.execute("user:42".to_string(), || Ok(String::from("never runs")))
});

assert_eq!(leader.join().unwrap().unwrap().as_ref(), "profile data");
assert_eq!(waiter.join().unwrap().unwrap().as_ref(), "profile data");
```

The first caller executes the function; everyone else with the same key blocks and receives a shared handle (`Arc<V>`) to the same result. Errors are shared the same way.

## API

- `execute(key, f)` — deduplicated call; the result is wrapped in `Arc` so waiters clone a pointer, not the payload.
- `forget(key)` — drop the in-flight entry; the next `execute` for that key starts fresh.
- `in_flight()` — how many keys currently have a running call.

## Semantics

- Results and errors are deduplicated independently: a failed call is shared to all waiters of that flight.
- If the leader's function panics, the leader re-raises the panic normally; waiters retry the call themselves instead of hanging or sharing a poisoned result.
- `forget` while a call is in flight does not disturb the running one — only the next call re-executes.

## License

Licensed under either of MIT or Apache-2.0, at your option.

Name is a tribute to Go's [x/sync/singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight); this crate has no code shared with it and is an independent implementation for Rust.
