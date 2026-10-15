// SPDX-License-Identifier: AGPL-3.0-or-later

//! `POST /cameras/:id/record-now` — the externally triggered recording endpoint
//! (migration 0083). It writes footage to disk that would otherwise evaporate
//! from the motion RAM ring, so every gate in front of it gets a test: the
//! deny-by-default `record_now` capability, per-camera scope, the per-camera
//! `motion_api_enabled` switch, body validation, and the durable request row
//! plus `events` surfacing the recorder relies on.
//!
//! Same harness as `ha_action_rbac.rs`: the real handler, the real RBAC code,
//! a real Postgres. There is no recorder in the harness, so the tests stop at
//! the contract boundary: the `motion_api_requests` row the recorder's
//! `api_trigger` loop consumes.

mod support;

use axum::http::StatusCode;
use crumb_common::{
    db,
    types::{BookmarkScope, Capabilities},
};
use support::*;
use uuid::Uuid;

/// A normal viewer PLUS `record_now` (what a dedicated integration service
/// user's role looks like).
fn caps_with_record_now() -> Capabilities {
    Capabilities {
        export: false,
        playback: true,
        clips: true,
        ptz: false,
        bookmarks: BookmarkScope::Own,
        manage_views: true,
        view_plates: false,
        actuators: false,
        manage_channels: false,
        record_now: true,
    }
}

async fn seed_trigger_user(pool: &deadpool_postgres::Pool, cameras: &[Uuid]) -> SeededUser {
    let role_id = seed_viewer_role_with_caps(pool, cameras, caps_with_record_now()).await;
    seed_viewer_user(pool, role_id).await
}

/// Flip the per-camera `api` source on (what `PUT /config/cameras/:id
/// {"motion_api_enabled": true}` does), straight in the DB.
async fn enable_api_source(pool: &deadpool_postgres::Pool, camera_id: Uuid) {
    let client = pool.get().await.expect("pool.get");
    client
        .execute(
            "UPDATE cameras SET motion_api_enabled = true WHERE id = $1",
            &[&camera_id],
        )
        .await
        .expect("enable motion_api_enabled");
}

fn body(seconds: i64) -> serde_json::Value {
    serde_json::json!({ "seconds": seconds, "label": "gate denied" })
}

async fn body_json(resp: axum::http::Response<axum::body::Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("json body")
}

// ─── 401: no credentials ─────────────────────────────────────────────────────

#[tokio::test]
async fn record_now_without_a_token_is_401() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/cameras/{cam}/record-now"))
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body(30).to_string()))
        .unwrap();
    assert_eq!(app.send(req).await.status(), StatusCode::UNAUTHORIZED);
}

// ─── 403: capability and camera scope ────────────────────────────────────────

#[tokio::test]
async fn record_now_denied_without_the_capability() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    enable_api_source(app.pool(), cam).await;
    // A generous viewer WITH access to the camera but no `record_now`.
    let viewer = seed_viewer(app.pool(), &[cam]).await;
    let token = login(&app, &viewer.username, &viewer.password).await;
    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{cam}/record-now"),
            &token,
            &body(30),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    // Nothing was written.
    let open = db::list_open_motion_api_requests(app.pool(), cam, chrono::Utc::now())
        .await
        .expect("list");
    assert!(open.is_empty());
}

#[tokio::test]
async fn record_now_denied_on_a_camera_outside_the_role_scope() {
    let app = TestApp::new().await;
    let mine = seed_camera(app.pool()).await;
    let other = seed_camera(app.pool()).await;
    enable_api_source(app.pool(), other).await;
    let user = seed_trigger_user(app.pool(), &[mine]).await;
    let token = login(&app, &user.username, &user.password).await;
    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{other}/record-now"),
            &token,
            &body(30),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// ─── 409: the camera has not opted into the api source ───────────────────────

#[tokio::test]
async fn record_now_is_409_until_the_api_source_is_enabled() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    let user = seed_trigger_user(app.pool(), &[cam]).await;
    let token = login(&app, &user.username, &user.password).await;
    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{cam}/record-now"),
            &token,
            &body(30),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

// ─── 400: validation ─────────────────────────────────────────────────────────

#[tokio::test]
async fn record_now_rejects_an_out_of_range_window() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    enable_api_source(app.pool(), cam).await;
    let user = seed_trigger_user(app.pool(), &[cam]).await;
    let token = login(&app, &user.username, &user.password).await;
    for secs in [0, -5, 3601] {
        let resp = app
            .send(post_auth_json(
                &format!("/cameras/{cam}/record-now"),
                &token,
                &body(secs),
            ))
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "seconds={secs}");
    }
    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{cam}/record-now"),
            &token,
            &serde_json::json!({ "seconds": 10, "pre_seconds": 61 }),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ─── 202: the request row the recorder consumes ──────────────────────────────

#[tokio::test]
async fn record_now_writes_the_request_row_with_pre_roll_folded_into_started_at() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    enable_api_source(app.pool(), cam).await;
    let user = seed_trigger_user(app.pool(), &[cam]).await;
    let token = login(&app, &user.username, &user.password).await;

    let before = chrono::Utc::now();
    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{cam}/record-now"),
            &token,
            &serde_json::json!({ "seconds": 45, "label": "  gate denied  ", "pre_seconds": 5 }),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let json = body_json(resp).await;
    assert_eq!(json["camera_id"], serde_json::json!(cam));
    assert_eq!(json["label"], serde_json::json!("gate denied"));
    assert_eq!(json["pre_seconds"], serde_json::json!(5));
    // The seeded default policy is what the camera inherits; the response
    // echoes its pre/post roll so a caller can predict the footage window.
    assert!(json["policy_pre_seconds"].is_number());
    assert!(json["policy_post_seconds"].is_number());
    assert!(matches!(
        json["mode"].as_str(),
        Some("motion" | "continuous")
    ));
    let request_id: Uuid = serde_json::from_value(json["request_id"].clone()).expect("request_id");

    let row = db::get_motion_api_request(app.pool(), request_id)
        .await
        .expect("get")
        .expect("row exists");
    assert_eq!(row.camera_id, cam);
    assert_eq!(row.requested_by, Some(user.user_id));
    assert_eq!(row.label.as_deref(), Some("gate denied"));
    assert_eq!(row.pre_seconds, 5);
    // started_at = request time - pre_seconds; ends_at = request time + seconds.
    let window = row.ends_at - row.started_at;
    assert_eq!(window.num_seconds(), 50);
    assert!(row.started_at <= before - chrono::Duration::seconds(4));
    assert!(row.ends_at >= before + chrono::Duration::seconds(44));

    // The recorder's catch-up query sees it while the window is open, and not
    // once it has elapsed.
    let open = db::list_open_motion_api_requests(app.pool(), cam, chrono::Utc::now())
        .await
        .expect("list open");
    assert_eq!(open.iter().filter(|r| r.id == request_id).count(), 1);
    let later = db::list_open_motion_api_requests(
        app.pool(),
        cam,
        chrono::Utc::now() + chrono::Duration::seconds(60),
    )
    .await
    .expect("list later");
    assert!(later.iter().all(|r| r.id != request_id));
}

#[tokio::test]
async fn admin_implies_record_now_and_the_flag_round_trips_through_the_camera_config() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    let admin = seed_admin(app.pool()).await;
    let token = login(&app, &admin.username, &admin.password).await;

    // Off by default → 409 even for an admin.
    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{cam}/record-now"),
            &token,
            &body(10),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // Enable through the real camera-config PUT, then it is accepted.
    let resp = app
        .send(put_auth_json(
            &format!("/config/cameras/{cam}"),
            &token,
            &serde_json::json!({ "motion_api_enabled": true }),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["motion_api_enabled"], serde_json::json!(true));

    let resp = app
        .send(post_auth_json(
            &format!("/cameras/{cam}/record-now"),
            &token,
            &body(10),
        ))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

// ─── events surfacing used by the recorder ───────────────────────────────────

#[tokio::test]
async fn api_event_upsert_opens_then_closes_one_labeled_row() {
    let app = TestApp::new().await;
    let cam = seed_camera(app.pool()).await;
    let started = chrono::Utc::now() - chrono::Duration::seconds(30);
    let ids = [Uuid::new_v4()];
    let a = db::upsert_api_event(app.pool(), cam, "gate denied", started, None, &ids)
        .await
        .expect("open");
    let b = db::upsert_api_event(
        app.pool(),
        cam,
        "gate denied",
        started,
        Some(started + chrono::Duration::seconds(20)),
        &ids,
    )
    .await
    .expect("close");
    assert_eq!(a, b, "START and STOP share one events row");
}
