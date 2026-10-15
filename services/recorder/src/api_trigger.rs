// SPDX-License-Identifier: AGPL-3.0-or-later

//! `api` motion source — externally triggered recording ("record now").
//!
//! The fourth additive motion source beside pixel ([`crate::motion`]), Frigate
//! ([`crate::frigate_motion`]) and Home Assistant ([`crate::ha_motion`]). An
//! integrator (an access-control or parking system that has just seen an
//! event) calls `POST /cameras/:id/record-now {seconds, label?}` on the api; the
//! api writes a `motion_api_requests` row and raises `pg_notify` on
//! [`MOTION_API_NOTIFY_CHANNEL`]; this module turns the row into the same
//! [`MotionSignal`](crumb_common::MotionSignal) START/STOP edges every other
//! source emits, so a Motion-mode camera persists its RAM pre-roll and records
//! until the window closes. `recording.rs` does the rest; nothing in the footage
//! path changes.
//!
//! # Transport: Postgres LISTEN/NOTIFY over a durable table
//!
//! The recorder has no HTTP server and must not grow one (golden rule 1: no new
//! ports). The api and the recorder already share exactly one thing, the
//! database, so the request travels through it:
//!
//! * the **table** `motion_api_requests` is the source of truth. A source loop
//!   (re)loads every request whose window is still open when it starts, so a
//!   request made while the recorder was down, or while the LISTEN connection
//!   was reconnecting, is honoured for the remainder of its window; and
//! * the **notification** is only the wake-up: one process-wide [`Hub`] holds a
//!   dedicated `LISTEN` connection (a pooled connection would be recycled under
//!   it, exactly like the recorder singleton lock) and fans each notice out to
//!   the per-camera loops over a broadcast channel. A lagged or lost notice
//!   degrades to the catch-up query, never to a lost request.
//!
//! # Health (fail-open)
//!
//! The source reports HEALTHY only while the hub's LISTEN connection is up and
//! the initial catch-up query succeeded. When the connection drops the loop
//! exits `Err`, so the camera fails OPEN (records everything) exactly like a
//! dead pixel detector: "cannot tell whether someone asked for footage"
//! resolves to "keep it". A camera whose ONLY source is `api` is therefore
//! motion-gated on the integrator's requests while the recorder can hear them
//! and records continuously while it cannot. Over-record, never loss (golden
//! rule 2, `docs/RECORDER-CORRECTNESS.md` invariant 19).

use std::sync::OnceLock;

use anyhow::{anyhow, Context as _, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_postgres::{AsyncMessage, NoTls};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crumb_common::db::{self, MotionApiRequest, MOTION_API_NOTIFY_CHANNEL};
use crumb_common::Camera;

use crate::frigate_motion::{emit, Transition};
use crate::MotionTx;

/// Back-off ceiling for the LISTEN reconnect loop (seconds).
const LISTEN_BACKOFF_MAX_SECS: u64 = 30;
/// Liveness probe interval on the LISTEN connection. A half-open TCP session
/// would otherwise sit silent forever; the probe turns it into an error, a
/// reconnect, and a fail-open window for every api-source camera meanwhile.
const LISTEN_PING_SECS: u64 = 30;
/// Broadcast capacity. A lagged subscriber re-reads its open requests from the
/// table, so this only bounds memory, never correctness.
const NOTICE_CAPACITY: usize = 1024;

// ─── hub ──────────────────────────────────────────────────────────────────────

/// One decoded `NOTIFY` payload: a new request for one camera.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Notice {
    pub camera_id: Uuid,
    pub request_id: Uuid,
}

/// The process-wide LISTEN connection, shared by every camera's `api` source
/// loop. Started once from `main` after the single-writer lock is held.
pub struct Hub {
    notices: broadcast::Sender<Notice>,
    connected: watch::Receiver<bool>,
}

static HUB: OnceLock<Hub> = OnceLock::new();

impl Hub {
    /// Start the hub: a background task that holds `LISTEN` on a dedicated
    /// connection, reconnecting with back-off, and fans notices out. Idempotent
    /// (a second call logs and does nothing).
    pub fn start(database_url: String, cancel: CancellationToken) {
        let (notices, _) = broadcast::channel(NOTICE_CAPACITY);
        let (conn_tx, conn_rx) = watch::channel(false);
        let hub = Hub {
            notices: notices.clone(),
            connected: conn_rx,
        };
        if HUB.set(hub).is_err() {
            warn!("api trigger hub already started; ignoring second start");
            return;
        }
        tokio::spawn(listen_loop(database_url, notices, conn_tx, cancel));
    }

    /// The hub, if `start` has run (tests and the `seed` binary never start one;
    /// a camera's `api` source then stays unhealthy = fail-open).
    pub fn global() -> Option<&'static Hub> {
        HUB.get()
    }

    /// Subscribe to notices for every camera; filter on `camera_id`.
    pub fn subscribe(&self) -> broadcast::Receiver<Notice> {
        self.notices.subscribe()
    }

    /// `true` while the LISTEN connection is up and subscribed.
    pub fn connected(&self) -> watch::Receiver<bool> {
        self.connected.clone()
    }
}

async fn listen_loop(
    database_url: String,
    notices: broadcast::Sender<Notice>,
    conn_tx: watch::Sender<bool>,
    cancel: CancellationToken,
) {
    let mut backoff_secs: u64 = 1;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let started = std::time::Instant::now();
        match run_listener(&database_url, &notices, &conn_tx, &cancel).await {
            Ok(()) => break, // cancelled
            Err(e) => {
                let _ = conn_tx.send(false);
                // A session that lived a while earned a fresh back-off; a session
                // that died at once keeps climbing toward the ceiling.
                if started.elapsed().as_secs() >= LISTEN_BACKOFF_MAX_SECS {
                    backoff_secs = 1;
                }
                warn!(
                    error = %e,
                    retry_in_secs = backoff_secs,
                    "api trigger listener ended; api-source cameras fail open until it reconnects"
                );
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                }
                backoff_secs = (backoff_secs * 2).min(LISTEN_BACKOFF_MAX_SECS);
            }
        }
    }
    let _ = conn_tx.send(false);
    info!("api trigger listener exiting");
}

/// One LISTEN session: connect, subscribe, pump notifications until the
/// connection ends or `cancel` fires. `Ok(())` only on cancellation.
async fn run_listener(
    database_url: &str,
    notices: &broadcast::Sender<Notice>,
    conn_tx: &watch::Sender<bool>,
    cancel: &CancellationToken,
) -> Result<()> {
    let (client, mut connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .context("api trigger: dedicated connect")?;

    // Drive the connection on its own task and forward notifications. The
    // client below stays free for LISTEN and the liveness probe; if this task
    // ends, the next client call errors and the session is rebuilt.
    let (ntx, mut nrx) = mpsc::channel::<tokio_postgres::Notification>(NOTICE_CAPACITY);
    let conn_task = tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                None => break,
                Some(Err(e)) => {
                    warn!(error = %e, "api trigger: LISTEN connection error");
                    break;
                }
                Some(Ok(AsyncMessage::Notification(n))) => {
                    if ntx.send(n).await.is_err() {
                        break;
                    }
                }
                // `AsyncMessage` is non-exhaustive; server notices are ignored.
                Some(Ok(_)) => {}
            }
        }
    });
    // Whatever happens below, never leak the connection task.
    struct AbortOnDrop(tokio::task::JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _guard = AbortOnDrop(conn_task);

    client
        .batch_execute(&format!("LISTEN {MOTION_API_NOTIFY_CHANNEL}"))
        .await
        .context("api trigger: LISTEN")?;
    let _ = conn_tx.send(true);
    info!(
        channel = MOTION_API_NOTIFY_CHANNEL,
        "api trigger listener connected"
    );

    let mut ping = tokio::time::interval(std::time::Duration::from_secs(LISTEN_PING_SECS));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await; // the first tick fires immediately; skip it

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            msg = nrx.recv() => {
                let Some(n) = msg else {
                    return Err(anyhow!("LISTEN connection closed"));
                };
                match parse_notice(n.payload()) {
                    Some(notice) => {
                        // Err = no subscriber right now (no api-source camera); fine.
                        let _ = notices.send(notice);
                    }
                    None => debug!(payload = n.payload(), "api trigger: unparseable notice"),
                }
            }
            _ = ping.tick() => {
                client
                    .simple_query("SELECT 1")
                    .await
                    .context("api trigger: liveness probe")?;
            }
        }
    }
}

/// Decode a `NOTIFY` payload (`{"camera_id": …, "request_id": …}`).
fn parse_notice(payload: &str) -> Option<Notice> {
    let v: serde_json::Value = serde_json::from_str(payload).ok()?;
    let camera_id = v.get("camera_id")?.as_str()?.parse().ok()?;
    let request_id = v.get("request_id")?.as_str()?.parse().ok()?;
    Some(Notice {
        camera_id,
        request_id,
    })
}

// ─── per-camera window tracker ────────────────────────────────────────────────

/// A transition the loop publishes as a [`MotionSignal`](crumb_common::MotionSignal)
/// plus a labeled `events` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiTransition {
    Start {
        started_at: DateTime<Utc>,
        label: String,
        request_ids: Vec<Uuid>,
    },
    Stop {
        started_at: DateTime<Utc>,
        stopped_at: DateTime<Utc>,
        label: String,
        request_ids: Vec<Uuid>,
    },
}

impl ApiTransition {
    fn to_signal_transition(&self) -> Transition {
        match self {
            ApiTransition::Start { started_at, .. } => Transition::Start {
                started_at: *started_at,
                peak_score: 1.0,
            },
            ApiTransition::Stop {
                started_at,
                stopped_at,
                ..
            } => Transition::Stop {
                started_at: *started_at,
                stopped_at: *stopped_at,
                peak_score: 1.0,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenWindow {
    started_at: DateTime<Utc>,
    ends_at: DateTime<Utc>,
    label: String,
    request_ids: Vec<Uuid>,
}

/// One camera's open record-now window, folded from one or more overlapping
/// requests. Pure and deterministic given an injected `now`, so it unit-tests
/// without a database or a clock. Overlapping requests EXTEND the open window
/// (one START, one STOP) rather than opening a second event: the buffer only
/// needs the union, and a single labeled `events` row reads better.
#[derive(Debug, Default)]
pub struct ApiTracker {
    open: Option<OpenWindow>,
}

/// The `events` label used when a request carries none.
pub const DEFAULT_LABEL: &str = "record";

impl ApiTracker {
    /// Fold a request in. Returns a START when it opens a window; `None` when
    /// it extends an open one or its window has already elapsed (nothing the
    /// buffer could still persist).
    pub fn observe(&mut self, req: &MotionApiRequest, now: DateTime<Utc>) -> Option<ApiTransition> {
        if req.ends_at <= now {
            return None;
        }
        match &mut self.open {
            Some(open) => {
                if req.ends_at > open.ends_at {
                    open.ends_at = req.ends_at;
                }
                if !open.request_ids.contains(&req.id) {
                    open.request_ids.push(req.id);
                }
                None
            }
            None => {
                let label = req
                    .label
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(DEFAULT_LABEL)
                    .to_owned();
                self.open = Some(OpenWindow {
                    started_at: req.started_at,
                    ends_at: req.ends_at,
                    label: label.clone(),
                    request_ids: vec![req.id],
                });
                Some(ApiTransition::Start {
                    started_at: req.started_at,
                    label,
                    request_ids: vec![req.id],
                })
            }
        }
    }

    /// Close the window once `now` has reached its end.
    pub fn tick(&mut self, now: DateTime<Utc>) -> Option<ApiTransition> {
        let due = self.open.as_ref().is_some_and(|o| now >= o.ends_at);
        if !due {
            return None;
        }
        self.open.take().map(|o| ApiTransition::Stop {
            started_at: o.started_at,
            stopped_at: o.ends_at,
            label: o.label,
            request_ids: o.request_ids,
        })
    }

    /// Close an open window early (loop exit), so `recording.rs` never holds an
    /// api event across a reconnect. The post-roll still applies from `now`.
    pub fn force_stop(&mut self, now: DateTime<Utc>) -> Option<ApiTransition> {
        self.open.take().map(|o| ApiTransition::Stop {
            started_at: o.started_at,
            stopped_at: now.min(o.ends_at),
            label: o.label,
            request_ids: o.request_ids,
        })
    }

    /// When the open window ends, if any.
    pub fn next_deadline(&self) -> Option<DateTime<Utc>> {
        self.open.as_ref().map(|o| o.ends_at)
    }
}

/// Emit a transition: the footage-driving `MotionSignal` FIRST, then the
/// best-effort labeled `events` row (never gates or delays the signal).
async fn publish(motion_tx: &MotionTx, pool: &Pool, camera_id: Uuid, t: &ApiTransition) {
    emit(motion_tx, camera_id, t.to_signal_transition());
    let (label, started_at, stopped_at, ids) = match t {
        ApiTransition::Start {
            started_at,
            label,
            request_ids,
        } => (label, *started_at, None, request_ids),
        ApiTransition::Stop {
            started_at,
            stopped_at,
            label,
            request_ids,
        } => (label, *started_at, Some(*stopped_at), request_ids),
    };
    if let Err(e) = db::upsert_api_event(pool, camera_id, label, started_at, stopped_at, ids).await
    {
        warn!(camera_id = %camera_id, error = %e, "failed to persist api motion event (surfacing only)");
    }
}

// ─── per-camera source loop ───────────────────────────────────────────────────

/// Run the `api` motion source for one camera until `cancel` fires or the hub's
/// LISTEN connection drops. Returns `Ok(())` on cancel / not-started (the caller
/// idles and re-checks) and `Err` when the listener is lost or the catch-up
/// query fails, so the supervisor fails the camera OPEN and respawns with
/// back-off. Reports HEALTHY only after the listener is up AND the catch-up
/// query succeeded, never optimistically before.
#[allow(clippy::too_many_arguments)]
pub async fn run_api_motion_loop(
    camera: &Camera,
    motion_tx: &MotionTx,
    cancel: &CancellationToken,
    pool: &Pool,
    health_tx: &crate::MotionHealthTx,
    alert_gate: &std::sync::Arc<crate::motion::UnhealthyAlertGate>,
    alert_after_secs: u64,
) -> Result<()> {
    let Some(hub) = Hub::global() else {
        crate::motion::report_health(
            health_tx,
            pool,
            camera.id,
            false,
            "api trigger hub not started",
            alert_gate,
            alert_after_secs,
        )
        .await;
        tokio::select! {
            () = cancel.cancelled() => {}
            () = tokio::time::sleep(std::time::Duration::from_secs(LISTEN_BACKOFF_MAX_SECS)) => {}
        }
        return Ok(());
    };
    let mut notices = hub.subscribe();
    let mut connected = hub.connected();

    // Wait for the listener before claiming anything.
    while !*connected.borrow() {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            r = connected.changed() => {
                if r.is_err() {
                    return Err(anyhow!("api trigger hub closed"));
                }
            }
        }
    }

    // Durable catch-up: every request whose window is still open, including any
    // made while this loop (or the whole recorder) was down.
    let mut tracker = ApiTracker::default();
    let now = Utc::now();
    let open = db::list_open_motion_api_requests(pool, camera.id, now)
        .await
        .context("api trigger: load open requests")?;
    crate::motion::report_health(
        health_tx,
        pool,
        camera.id,
        true,
        "api trigger listener connected",
        alert_gate,
        alert_after_secs,
    )
    .await;
    info!(camera_id = %camera.id, open_requests = open.len(), "api motion: listening for record-now requests");
    for req in &open {
        if let Some(t) = tracker.observe(req, now) {
            publish(motion_tx, pool, camera.id, &t).await;
        }
    }

    let result: Result<()> = loop {
        let deadline = tracker.next_deadline();
        let sleep_for = deadline.map(|d| {
            let ms = (d - Utc::now()).num_milliseconds().max(0);
            std::time::Duration::from_millis(u64::try_from(ms).unwrap_or(0))
        });
        tokio::select! {
            () = cancel.cancelled() => break Ok(()),
            r = connected.changed() => {
                if r.is_err() || !*connected.borrow() {
                    break Err(anyhow!("api trigger listener disconnected"));
                }
            }
            n = notices.recv() => match n {
                Ok(notice) if notice.camera_id == camera.id => {
                    match db::get_motion_api_request(pool, notice.request_id).await {
                        Ok(Some(req)) => {
                            if let Some(t) = tracker.observe(&req, Utc::now()) {
                                publish(motion_tx, pool, camera.id, &t).await;
                            }
                        }
                        Ok(None) => warn!(camera_id = %camera.id, request_id = %notice.request_id, "api motion: notified request not found"),
                        Err(e) => break Err(e.context("api trigger: load notified request")),
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // Missed notices: fall back to the table (the source of truth).
                    warn!(camera_id = %camera.id, missed = n, "api motion: notices lagged; reloading open requests");
                    let now = Utc::now();
                    match db::list_open_motion_api_requests(pool, camera.id, now).await {
                        Ok(reqs) => {
                            for req in &reqs {
                                if let Some(t) = tracker.observe(req, now) {
                                    publish(motion_tx, pool, camera.id, &t).await;
                                }
                            }
                        }
                        Err(e) => break Err(e.context("api trigger: reload open requests")),
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break Err(anyhow!("api trigger hub closed")),
            },
            () = async {
                match sleep_for {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                if let Some(t) = tracker.tick(Utc::now()) {
                    publish(motion_tx, pool, camera.id, &t).await;
                }
            }
        }
    };

    // Close any in-progress window so recording.rs doesn't hold it across a
    // reconnect (the post-roll still applies; a respawned loop re-opens it from
    // the table if the window has not elapsed).
    if let Some(t) = tracker.force_stop(Utc::now()) {
        publish(motion_tx, pool, camera.id, &t).await;
    }
    if let Err(e) = &result {
        error!(camera_id = %camera.id, error = %e, "api motion loop exiting with error");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn req(id: u128, start: i64, end: i64, label: Option<&str>) -> MotionApiRequest {
        MotionApiRequest {
            id: Uuid::from_u128(id),
            camera_id: Uuid::nil(),
            requested_by: None,
            label: label.map(str::to_owned),
            pre_seconds: 0,
            started_at: at(start),
            ends_at: at(end),
            created_at: at(start),
        }
    }

    #[test]
    fn first_request_opens_a_window_and_tick_closes_it_at_ends_at() {
        let mut t = ApiTracker::default();
        let r = req(1, 10, 40, Some("gate denied"));
        assert_eq!(
            t.observe(&r, at(10)),
            Some(ApiTransition::Start {
                started_at: at(10),
                label: "gate denied".to_owned(),
                request_ids: vec![Uuid::from_u128(1)],
            })
        );
        assert_eq!(t.next_deadline(), Some(at(40)));
        assert_eq!(t.tick(at(39)), None);
        assert_eq!(
            t.tick(at(40)),
            Some(ApiTransition::Stop {
                started_at: at(10),
                stopped_at: at(40),
                label: "gate denied".to_owned(),
                request_ids: vec![Uuid::from_u128(1)],
            })
        );
        assert_eq!(t.next_deadline(), None);
    }

    #[test]
    fn overlapping_request_extends_without_a_second_start() {
        let mut t = ApiTracker::default();
        assert!(t.observe(&req(1, 0, 30, None), at(0)).is_some());
        // A second request while open: no new START, the window grows.
        assert_eq!(t.observe(&req(2, 20, 60, Some("x")), at(20)), None);
        assert_eq!(t.next_deadline(), Some(at(60)));
        // A shorter overlapping request never shrinks the window.
        assert_eq!(t.observe(&req(3, 25, 40, None), at(25)), None);
        assert_eq!(t.next_deadline(), Some(at(60)));
        let stop = t.tick(at(60)).expect("stop");
        match stop {
            ApiTransition::Stop {
                started_at,
                stopped_at,
                label,
                request_ids,
            } => {
                assert_eq!(started_at, at(0));
                assert_eq!(stopped_at, at(60));
                assert_eq!(label, DEFAULT_LABEL);
                assert_eq!(request_ids.len(), 3);
            }
            ApiTransition::Start { .. } => panic!("expected stop"),
        }
    }

    #[test]
    fn elapsed_request_is_ignored() {
        let mut t = ApiTracker::default();
        assert_eq!(t.observe(&req(1, 0, 10, None), at(10)), None);
        assert_eq!(t.observe(&req(1, 0, 10, None), at(11)), None);
        assert_eq!(t.next_deadline(), None);
    }

    #[test]
    fn pre_roll_is_a_started_at_in_the_past() {
        // The api folds `pre_seconds` into started_at; the tracker passes it
        // through so the buffer's pre-window is measured from there.
        let mut t = ApiTracker::default();
        let start = t.observe(&req(1, 95, 130, None), at(100)).expect("start");
        assert!(matches!(start, ApiTransition::Start { started_at, .. } if started_at == at(95)));
    }

    #[test]
    fn force_stop_closes_at_now_not_later_than_ends_at() {
        let mut t = ApiTracker::default();
        assert!(t.observe(&req(1, 0, 100, None), at(0)).is_some());
        assert!(matches!(
            t.force_stop(at(10)),
            Some(ApiTransition::Stop { stopped_at, .. }) if stopped_at == at(10)
        ));
        assert_eq!(t.force_stop(at(11)), None);
    }

    #[test]
    fn parse_notice_round_trip() {
        let cam = Uuid::from_u128(7);
        let id = Uuid::from_u128(9);
        let payload = serde_json::json!({ "camera_id": cam, "request_id": id }).to_string();
        assert_eq!(
            parse_notice(&payload),
            Some(Notice {
                camera_id: cam,
                request_id: id
            })
        );
        assert_eq!(parse_notice("not json"), None);
        assert_eq!(parse_notice(r#"{"camera_id":"nope"}"#), None);
    }
}
