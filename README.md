# Swim

A generic connection pool with async/await support.

Inspired by [a Mobc rewrite that never happened](https://github.com/importcjj/mobc/pull/60#issuecomment-879616110).

# Example

```rust
use swim::{Pool, PoolConfig};

#[tokio::main]
async fn main() {
    let manager = FooConnectionManager::new("foo://localhost:0000"); // Imaginary "foo" DB.
    let pool = Pool::new(manager, PoolConfig::default().max_open(50));

    for i in 0..20 {
        let pool = pool.clone();
        tokio::spawn(async move {
            let client = pool.get().await.unwrap();
            // use the connection
            // it will be returned to the pool when it falls out of scope.
        });
    }
}
```

# Config

The config must be passed to the pool upon creation. It comes with default params that can be changed through a fluent API.
```rust
let config = PoolConfig::default()
  .max_open(20)
  .cleanup_rate(Duration::from_secs(5 * 60));
```

|Param|Type|Description|Default|
|-----|----|-----------|-------|
|max_open|`u16`|Maximum number of connections managed by the pool. Defaults to 10.
|max_idle|`Option<u16>`|Maximum idle connection count maintained by the pool.|None, meaning reuse forever
|max_lifetime|`Option<Duration>`|Maximum lifetime of connections in the pool. |None, meaning reuse forever
|idle_timeout|`Option<Duration>`|Maximum lifetime of idle connections in the pool.|5min
|cleanup_rate|`Duration`|Rate at which a connection cleanup is scheduled. |60sec
|test_on_check_out|`bool`|Check the connection before returning it to the client.|false
|get_timeout|`Duration`|Maximum time to wait for a connection to become available before returning an error.|30sec
