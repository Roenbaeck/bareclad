//! Asynchronous/Threaded interface for submitting and controlling Traqula queries.
//!
//! This module provides a minimal, thread-per-query runner that accepts Traqula
//! scripts, executes them on a background thread, and optionally streams results
//! back to the caller. It uses cooperative cancellation via an `Arc<AtomicBool>`.
//!
//! The goal is to keep threading concerns here without invasive changes to the
//! engine. Callers can submit queries and cancel them by id.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::sync::mpsc::{self, Sender, Receiver};

use crate::construct::Database;
use crate::traqula::Engine;

/// A single row emitted by the engine. For now it's just a line of text (stdout-compatible).
/// This can be evolved into a structured enum once projection returns tuples.
#[derive(Debug, Clone)]
pub struct Row(pub String);

/// Cancellation token shared with the worker thread.
#[derive(Debug)]
pub struct CancelToken(Arc<AtomicBool>);
impl CancelToken {
    pub fn new() -> Self { Self(Arc::new(AtomicBool::new(false))) }
    pub fn cancel(&self) { self.0.store(true, Ordering::SeqCst); }
    pub fn is_cancelled(&self) -> bool { self.0.load(Ordering::Relaxed) }
    pub fn clone(&self) -> Self { Self(Arc::clone(&self.0)) }
}

/// Opaque query identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(u64);

/// Handle to a running or completed query.
pub struct QueryHandle {
    pub id: QueryId,
    cancel: CancelToken,
    started: Instant,
    join: Option<JoinHandle<()>>,
    pub results: Option<Receiver<Row>>, // None when sink is stdout
}
impl QueryHandle {
    /// Request cancellation (cooperative). The worker may take a short time to observe it.
    pub fn cancel(&self) { self.cancel.cancel(); }
    /// Wait for the query to finish.
    pub fn join(mut self) { if let Some(j) = self.join.take() { let _ = j.join(); } }
    /// Elapsed time since start.
    pub fn elapsed(&self) -> Duration { self.started.elapsed() }
}

/// Query submission options.
pub struct QueryOptions {
    pub stream_results: bool,
    pub timeout: Option<Duration>,
}
impl Default for QueryOptions {
    fn default() -> Self { Self { stream_results: true, timeout: None } }
}

/// Registry managing query lifecycles.
pub struct QueryInterface<'db> {
    db: Arc<Database<'db>>, // shared database
    next_id: Mutex<u64>,
    active: Mutex<HashMap<QueryId, CancelToken>>, // for external cancellation
}

impl<'db> QueryInterface<'db> {
    pub fn new(db: Arc<Database<'db>>) -> Self {
        Self { db, next_id: Mutex::new(0), active: Mutex::new(HashMap::new()) }
    }

    fn allocate_id(&self) -> QueryId {
        let mut g = self.next_id.lock().unwrap();
        *g += 1; QueryId(*g)
    }

    /// Submit a Traqula script for execution on a background thread.
    /// When `options.stream_results` is true, a channel is returned for rows.
    pub fn start_query(&self, script: String, options: QueryOptions) -> QueryHandle {
        let id = self.allocate_id();
        let cancel = CancelToken::new();
        let cancel_for_registry = cancel.clone();
        self.active.lock().unwrap().insert(id, cancel_for_registry);

        let (tx, rx) = if options.stream_results { let (tx, rx) = mpsc::channel(); (Some(tx), Some(rx)) } else { (None, None) };

        let db = Arc::clone(&self.db);
        let cancel_for_thread = cancel.clone();
        let timeout = options.timeout;
        let join = thread::spawn(move || {
            let engine = Engine::new(&db);
            // Simple cooperative cancel by timeout
            let started = Instant::now();
            // Output adapter: either send to channel or print
            struct Sink { tx: Option<Sender<Row>> }
            impl Sink { fn emit(&self, s: String) { if let Some(ref tx) = self.tx { let _ = tx.send(Row(s)); } else { println!("{}", s); } } }
            let sink = Sink { tx };

            // For now we don't thread cancellation through Engine; as a stopgap, we split the script
            // and execute linearly, checking timeout in-between commands.
            let parts = script.split(';');
            for part in parts {
                let p = part.trim();
                if p.is_empty() { continue; }
                if cancel_for_thread.is_cancelled() { break; }
                if let Some(d) = timeout { if started.elapsed() > d { break; } }
                // Execute one command chunk
                engine.execute(p);
            }
        });

        QueryHandle { id, cancel, started: Instant::now(), join: Some(join), results: rx }
    }

    /// Cancel a query by id.
    pub fn cancel(&self, id: QueryId) -> bool {
        if let Some(tok) = self.active.lock().unwrap().get(&id) {
            tok.cancel();
            true
        } else { false }
    }
}
