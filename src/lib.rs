use std::ops::Deref;

use async_trait::async_trait;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration, Instant};

#[async_trait]
/// A trait which provides connection-specific functionality.
pub trait Manager: Send + Sync + 'static {
    type Connection: Send + Sync + 'static;
    type Error: Send + Sync + 'static;

    /// Attempts to create a new connection.
    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error>;
    /// Checks if the connection is closed.
    /// The usual implementation checks if the underlying TCP socket has disconnected.
    /// This is called synchronously every time a connection is returned to the pool.
    fn is_closed(&self, conn: &mut Self::Connection) -> bool;
}

#[derive(Debug)]
pub struct Connection<M: Manager> {
    raw: Option<M::Connection>,
    created_at: Instant,
    last_used_at: Instant,

    recycle_tx: UnboundedSender<RecycleConnection<M>>,
}

pub struct RecycleConnection<M: Manager> {
    raw: M::Connection,
    created_at: Instant,
    last_used_at: Instant,
}

impl<M: Manager> Drop for Connection<M> {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.take() {
            let recycle_connection = RecycleConnection {
                raw,
                created_at: self.created_at.clone(),
                last_used_at: self.last_used_at.clone(),
            };

            if !self.recycle_tx.is_closed() {
                if let Err(e) = self.recycle_tx.send(recycle_connection) {
                    eprintln!("Application error: {e}");
                }
            }
        }
    }
}

impl<M: Manager> Deref for Connection<M> {
    type Target = M::Connection;
    fn deref(&self) -> &Self::Target {
        self.raw.as_ref().unwrap()
    }
}

#[derive(Debug)]
pub struct PoolError;

pub struct PoolConfig {
    // Maximum number of connections managed by the pool
    pub max_open: u16,
    // Maximum idle connection count maintained by the pool
    pub max_idle: Option<u16>,
    // Maximum lifetime of connections in the pool
    pub max_lifetime: Option<Duration>,
    // Maximum lifetime of idle connections in the pool
    pub idle_timeout: Option<Duration>,
    // Maximum time to wait for a connection to become available before returning an error
    pub get_timeout: Duration,
    // Rate at which a connection cleanup is scheduled
    pub cleanup_rate: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            max_open: 10,
            max_idle: None,
            max_lifetime: None,
            idle_timeout: Some(Duration::from_secs(5 * 60)),
            get_timeout: Duration::from_secs(30),
            cleanup_rate: Duration::from_secs(60),
        }
    }
}

impl PoolConfig {
    pub fn max_open(mut self, max_open: u16) -> Self {
        self.max_open = max_open;
        self
    }

    pub fn max_idle(mut self, max_idle: Option<u16>) -> Self {
        self.max_idle = max_idle;
        self
    }

    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn get_timeout(mut self, get_timeout: Duration) -> Self {
        self.get_timeout = get_timeout;
        self
    }

    pub fn cleanup_rate(mut self, cleanup_rate: Duration) -> Self {
        self.cleanup_rate = cleanup_rate;
        self
    }
}

pub struct PoolConfigBackend {
    // Maximum number of connections managed by the pool
    pub max_open: u16,
    // Maximum idle connection count maintained by the pool
    pub max_idle: Option<u16>,
    // Maximum lifetime of connections in the pool
    pub max_lifetime: Option<Duration>,
    // Maximum lifetime of idle connections in the pool
    pub idle_timeout: Option<Duration>,
    // Rate at which a connection cleanup is scheduled
    pub cleanup_rate: Duration,
}

type GetConnectionCmd<M> = oneshot::Sender<Option<Connection<M>>>;
type GetStatsCmd = oneshot::Sender<PoolStats>;

pub struct Pool<M: Manager> {
    request_tx: UnboundedSender<GetConnectionCmd<M>>,
    stats_tx: Sender<GetStatsCmd>,
    get_timeout: Duration,
}

#[derive(Debug)]
pub struct PoolStats {
    pub nb_open_connections: usize,
    pub nb_idles_connections: usize,
}

struct PoolBackend<M: Manager> {
    config: PoolConfigBackend,
    request_rx: UnboundedReceiver<GetConnectionCmd<M>>,
    stats_tx: Receiver<GetStatsCmd>,

    idles: Vec<Connection<M>>,
    nb_open_connections: usize,
}

impl<M: Manager> PoolBackend<M> {
    pub fn new(
        config: PoolConfigBackend,
        request_rx: UnboundedReceiver<GetConnectionCmd<M>>,
        stats_tx: Receiver<GetStatsCmd>,
    ) -> PoolBackend<M> {
        PoolBackend::<M> {
            config,
            request_rx,
            stats_tx,
            idles: Vec::new(),
            nb_open_connections: 0,
        }
    }

    async fn run(mut self, manager: M) {
        // Channel to recycle connections
        let (recycle_tx, mut recycle_rx) = mpsc::unbounded_channel();

        // Channel to run idle connections cleanup loops
        // A cleanup event is queued every `cleanup_rate`
        let (clean_tx, mut clean_rx) = mpsc::channel(1);
        let initial_clean_tx = clean_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(self.config.cleanup_rate).await;
            initial_clean_tx.send(()).await
        });

        loop {
            select! {
                // When a user asks for a connection:
                // 1. If there is one available in the idle pool, we return it
                // 2. Otherwise, if the creation limit hasn't been reached yet, open a new connection and return it
                // 3. If we have reached this stage, simply return None
                Some(resp) = self.request_rx.recv() => {
                    let mut result = None;

                    if self.idles.len() > 0 {
                        let mut connection  = self.idles.swap_remove(0);
                        connection.last_used_at = Instant::now();

                        if connection.raw.is_some() && !manager.is_closed(connection.raw.as_mut().unwrap()) {
                            result = Some(connection);
                        }
                    }

                    if self.nb_open_connections < self.config.max_open.into() {
                        self.nb_open_connections += 1;

                        if let Ok(raw_connection) = manager.connect().await {
                            let connection = Connection {
                                raw: Some(raw_connection),
                                created_at: Instant::now(),
                                last_used_at: Instant::now(),
                                recycle_tx: recycle_tx.clone(),
                            };
                            result = Some(connection);
                        }
                    }

                    if let Err(_) = resp.send(result) {
                        eprintln!("Connection receiver dropped.");
                    };

                },
                // When a connection is droped, if it's max_lifetime & idle_timeout are still valid, we recycle it to the idle connection pool
                Some(conn) = recycle_rx.recv() => {

                    let now = Instant::now();
                    let time_since_conn_creation = now.duration_since(conn.created_at);
                    let time_since_conn_idle = now.duration_since(conn.last_used_at);

                    let is_max_lifetime_ok = self.config.max_lifetime.is_none() || time_since_conn_creation < self.config.max_lifetime.unwrap();
                    let is_idle_timeout_ok = self.config.idle_timeout.is_none() || time_since_conn_idle < self.config.idle_timeout.unwrap();
                    let is_max_idle_ok = self.config.max_idle.is_none() || self.idles.len() < self.config.max_idle.unwrap().into();

                    if is_max_lifetime_ok && is_max_idle_ok && is_idle_timeout_ok {
                        let connection = Connection {
                            raw: Some(conn.raw),
                            created_at: conn.created_at,
                            last_used_at: Instant::now(),
                            recycle_tx: recycle_tx.clone()
                        };
                        self.idles.push(connection);
                    } else {
                        self.nb_open_connections -= 1;
                    }
                },
                // Idle pool cleanup scheduling
                Some(_) = clean_rx.recv() => {
                    let now = Instant::now();

                    self.idles.retain(|conn| {
                        let is_lifetime_expired = conn.created_at + self.config.max_lifetime.unwrap_or(conn.created_at.elapsed()) < now;
                        let is_idle_expired = conn.last_used_at + self.config.idle_timeout.unwrap_or(conn.last_used_at.elapsed()) < now;

                        return !is_lifetime_expired && !is_idle_expired;
                    });

                    tokio::time::sleep(self.config.cleanup_rate).await;
                    if let Err(_) = clean_tx.send(()).await {
                        eprintln!("Cleaning receiver dropped.");
                    };
                },
                Some(resp) = self.stats_tx.recv() => {
                    let stats = PoolStats {
                        nb_open_connections: self.nb_open_connections,
                        nb_idles_connections: self.idles.len()
                    };
                    if let Err(_) = resp.send(stats) {
                        eprintln!("Connection receiver dropped.");
                    };

                }
            }
        }
    }
}

impl<M: Manager> Pool<M> {
    pub fn new(manager: M, config: PoolConfig) -> Pool<M> {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (stats_tx, stats_rx) = mpsc::channel(1);

        let backend = PoolBackend::new(
            PoolConfigBackend {
                max_idle: config.max_idle,
                idle_timeout: config.idle_timeout,
                max_lifetime: config.max_lifetime,
                max_open: config.max_open,
                cleanup_rate: config.cleanup_rate,
            },
            request_rx,
            stats_rx,
        );

        tokio::spawn(async move { backend.run(manager).await });

        Pool {
            stats_tx,
            request_tx,
            get_timeout: config.get_timeout,
        }
    }

    pub async fn stats(&self) -> PoolStats {
        let (resp_tx, resp_rx) = oneshot::channel();
        if let Err(err) = self.stats_tx.send(resp_tx).await {
            eprintln!("Failed to send stats request to backend - {err}");
            return PoolStats {
                nb_idles_connections: 0,
                nb_open_connections: 0,
            };
        }

        match resp_rx.await {
            Ok(result) => result,
            Err(err) => {
                eprintln!("No response from pool backend - {err}");
                PoolStats {
                    nb_idles_connections: 0,
                    nb_open_connections: 0,
                }
            }
        }
    }

    pub async fn try_get(&self) -> Option<Connection<M>> {
        let (resp_tx, resp_rx) = oneshot::channel();

        if let Err(err) = self.request_tx.send(resp_tx) {
            eprintln!("Failed to send connection request to backend - {err}");
        }

        match resp_rx.await {
            Ok(result) => match result {
                Some(connection) => Some(connection),
                None => None,
            },
            Err(err) => {
                eprintln!("No response from pool backend - {err}");
                None
            }
        }
    }

    pub async fn get(&self) -> Result<Connection<M>, tokio::time::error::Elapsed> {
        match timeout(self.get_timeout, async {
            loop {
                if let Some(connection) = self.try_get().await {
                    return connection;
                }
            }
        })
        .await
        {
            Ok(result) => Ok(result),
            Err(err) => {
                return Err(err);
            }
        }
    }
}

impl<M: Manager> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Pool {
            stats_tx: self.stats_tx.clone(),
            request_tx: self.request_tx.clone(),
            get_timeout: self.get_timeout.clone(),
        }
    }
}
