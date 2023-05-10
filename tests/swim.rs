use async_trait::async_trait;
use tokio::time::Duration;

use swim::{Manager, Pool, PoolConfig};

struct OkManager;
pub struct TestError;
struct TestConnection(bool);

#[async_trait]
impl Manager for OkManager {
    type Connection = TestConnection;
    type Error = TestError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(TestConnection(true))
    }
    fn is_open(&self, _: &mut Self::Connection) -> bool {
        true
    }
    async fn is_valid(&self, _: &mut Self::Connection) -> bool {
        true
    }
}

#[tokio::test]
async fn test_max_open_ok() {
    let pool = Pool::new(OkManager, PoolConfig::default().max_open(5));

    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().await.ok().unwrap());
    }
}

#[tokio::test]
async fn test_acquire_release() {
    let pool = Pool::new(OkManager, PoolConfig::default().max_open(2));

    let conn1 = pool.get().await.ok().unwrap();
    let conn2 = pool.get().await.ok().unwrap();
    drop(conn1);
    let conn3 = pool.get().await.ok().unwrap();
    drop(conn2);
    drop(conn3);
}

#[tokio::test]
async fn test_get_timeout() {
    // get_timeout is at 100ms
    let pool = Pool::new(
        OkManager,
        PoolConfig::default()
            .max_open(1)
            .get_timeout(Duration::from_millis(100)),
    );

    // Open 1 connection, pool is full
    let succeeds_immediately = pool.get().await;
    assert!(succeeds_immediately.is_ok());

    // Wait 50ms before droping the connection
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(succeeds_immediately);
    });

    // Get should succeeds after 50ms. This is still inside the timeout
    let succeeds_delayed = pool.get().await;
    assert!(succeeds_delayed.is_ok());

    // Wait 150ms before droping the connection
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(succeeds_delayed);
    });

    // Get should fail. We exceeded the 100ms window
    let fails = pool.get().await;
    assert!(fails.is_err());
}

#[tokio::test]
async fn test_idle_timeout_cleanup() {
    let pool = Pool::new(
        OkManager,
        PoolConfig::default()
            .max_open(1)
            .get_timeout(Duration::from_millis(100))
            .idle_timeout(Some(Duration::from_millis(50)))
            .cleanup_rate(Duration::from_millis(100)),
    );

    // Open 1 connection, pool is full
    let conn1 = pool.get().await;
    assert!(conn1.is_ok());
    drop(conn1);

    tokio::time::sleep(Duration::from_millis(50)).await;
    // The connection should have been recycled and added to the idle pool
    let before_clean = pool.stats().await;
    assert_eq!(before_clean.nb_open_connections, 1);
    assert_eq!(before_clean.nb_idles_connections, 1);

    // After 150ms, the open connection should have been cleaned
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_clean = pool.stats().await;
    assert_eq!(after_clean.nb_open_connections, 0);
    assert_eq!(after_clean.nb_idles_connections, 0);
}
