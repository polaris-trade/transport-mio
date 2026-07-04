# transport_mio

Mio-based Transport backend for Polaris networking stack

## Usage

```rust
use transport_mio::{DefaultGreeter, Greeter};

let g = DefaultGreeter;
let msg = g.greet("world").unwrap();
assert_eq!(msg, "hello, world");
```

## License

Dual-licensed under either [MIT](../../LICENSE-MIT) or [Apache 2.0](../../LICENSE-APACHE), at your option.
