// SPDX-License-Identifier: AGPL-3.0-or-later

//! `POST /cameras/:id/record-now` — externally triggered recording.
//!
//! The api half of the `api` motion source (recorder half:
//! `services/recorder/src/api_trigger.rs`; design: `docs/DECISIONS.md`
//! 2026-10-15 and `docs/MOTION-RECORDING.md` §9). An integration that has just
//! observed something worth keeping (an access denial, a barrier held, an
//! alarm) asks Crumb to record a camera for a window, and a Motion-mode camera
//! persists its RAM pre-roll plus `seconds` of footage exactly as it would for
//! a pixel or sensor trigger. A Continuous-mode camera already records; the
//! call still writes the labeled `events` row so the moment is findable on the
//! timeline and in `GET /events`.
//!
//! # Endpoint
//!
//! | Method | Path | Auth | Description |
//! |--------|------|------|-------------|
//! | `POST` | `/cameras/:id/record-now` | Bearer + `record_now` capability + camera access | Record `seconds` from now (plus pre-roll) |
//!
//! # Request body
//!
//! ```json
//! { "seconds": 30, "label": "gate denied 7F-1234", "pre_seconds": 5 }
//! ```
//!
//! * `seconds` — required, `1..=3600`: how long to keep recording from now. The
//!   policy's `motion_post_seconds` post-roll still applies after the window.
//! * `label` — optional, ≤120 chars, shown as the `events` row label (default
//!   `"record"`).
//! * `pre_seconds` — optional, `0..=60`: extra pre-roll on top of the policy's
//!   `motion_pre_seconds`, bounded by what the recorder's RAM ring still holds
//!   (about `motion_pre_seconds + 8 s`). Folded into `started_at`.
//!
//! # Responses
//!
//! * `202 Accepted` with the stored request: `request_id`, `camera_id`, `mode`
//!   (`"motion"` | `"continuous"`), `label`, `started_at`, `ends_at`,
//!   `pre_seconds`, `policy_pre_seconds`, `policy_post_seconds`. The recorder
//!   acts on it asynchronously (sub-second over `LISTEN/NOTIFY`); the footage
//!   itself appears in `/timeline` as segments close.
//! * `400` — invalid `seconds` / `pre_seconds` / `label`.
//! * `401` / `403` — unauthenticated; no `record_now` capability or no access to
//!   the camera. A scoped media token never carries `record_now`.
//! * `404` — unknown camera.
//! * `409` — the camera is disabled, or the `api` motion source is not enabled
//!   on it (`PUT /config/cameras/:id {"motion_api_enabled": true}`).
//!
//! # Why a capability and not "any viewer"
//!
//! A request writes footage to disk that would otherwise evaporate from RAM:
//! it costs the operator storage and it is the integrator's (not a viewer's)
//! action, so it is deny-by-default like `actuators` and `manage_channels`.
//! Admins imply it. The intended principal is a dedicated service user whose
//! role holds `record_now` plus exactly the cameras the integration may touch.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crumb_common::{db, RecordingMode};

use crate::{auth_mw::AuthUser, error::ApiError, state::AppState};

/// Longest window one request may ask for.
pub const MAX_SECONDS: i64 = 3600;
/// Longest extra pre-roll one request may ask for.
pub const MAX_PRE_SECONDS: i64 = 60;
/// Longest label.
pub const MAX_LABEL_CHARS: usize = 120;

/// `POST /cameras/:id/record-now` body.
#[derive(Debug, Deserialize)]
pub struct RecordNowRequest {
    pub seconds: i64,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub pre_seconds: Option<i64>,
}

/// `202` body: the stored request plus the policy figures the caller needs to
/// predict the footage window.
#[derive(Debug, Serialize)]
pub struct RecordNowResponse {
    pub request_id: Uuid,
    pub camera_id: Uuid,
    /// `"motion"` (pre-roll persisted + window recorded) or `"continuous"`
    /// (already recording; only the event marker is new).
    pub mode: &'static str,
    pub label: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub pre_seconds: i64,
    pub policy_pre_seconds: i32,
    pub policy_post_seconds: i32,
}

/// Mount the route (merged into the JSON route group in `main.rs`).
pub fn routes() -> Router<AppState> {
    Router::new().route("/cameras/:id/record-now", post(record_now))
}

/// Validate and normalise the body: `(seconds, pre_seconds, label)`.
fn validate(body: &RecordNowRequest) -> Result<(i64, i64, Option<String>), ApiError> {
    if !(1..=MAX_SECONDS).contains(&body.seconds) {
        return Err(ApiError::BadRequest(format!(
            "seconds must be between 1 and {MAX_SECONDS}"
        )));
    }
    let pre = body.pre_seconds.unwrap_or(0);
    if !(0..=MAX_PRE_SECONDS).contains(&pre) {
        return Err(ApiError::BadRequest(format!(
            "pre_seconds must be between 0 and {MAX_PRE_SECONDS}"
        )));
    }
    let label = body
        .label
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    if let Some(l) = &label {
        if l.chars().count() > MAX_LABEL_CHARS {
            return Err(ApiError::BadRequest(format!(
                "label must be at most {MAX_LABEL_CHARS} characters"
            )));
        }
        if l.chars().any(char::is_control) {
            return Err(ApiError::BadRequest(
                "label must not contain control characters".to_owned(),
            ));
        }
    }
    Ok((body.seconds, pre, label))
}

async fn record_now(
    user: AuthUser,
    State(state): State<AppState>,
    Path(camera_id): Path<Uuid>,
    Json(body): Json<RecordNowRequest>,
) -> Result<(StatusCode, Json<RecordNowResponse>), ApiError> {
    // Capability first, then camera scope (403 either way, never a 404 that
    // would confirm a camera exists to a caller who may not see it).
    user.require_record_now()?;
    user.assert_camera_access(camera_id)?;
    let (seconds, pre_seconds, label) = validate(&body)?;

    let cam = db::get_camera(state.pool(), camera_id)
        .await
        .map_err(ApiError::Internal)?
        .ok_or_else(|| ApiError::NotFound("camera not found".to_owned()))?;
    if !cam.enabled {
        return Err(ApiError::Conflict("camera is disabled".to_owned()));
    }
    if !cam.motion_api_enabled {
        return Err(ApiError::Conflict(
            "the api motion source is not enabled on this camera \
             (PUT /config/cameras/:id {\"motion_api_enabled\": true})"
                .to_owned(),
        ));
    }

    let now = Utc::now();
    let started_at = now - chrono::Duration::seconds(pre_seconds);
    let ends_at = now + chrono::Duration::seconds(seconds);
    let pre_i32 = i32::try_from(pre_seconds).unwrap_or(i32::MAX);
    let row = db::create_motion_api_request(
        state.pool(),
        &db::CreateMotionApiRequestParams {
            camera_id,
            requested_by: Some(user.user_id),
            label: label.as_deref(),
            pre_seconds: pre_i32,
            started_at,
            ends_at,
        },
    )
    .await
    .map_err(ApiError::Internal)?;

    let mode = match cam.policy.mode {
        RecordingMode::Motion => "motion",
        RecordingMode::Continuous => "continuous",
    };
    Ok((
        StatusCode::ACCEPTED,
        Json(RecordNowResponse {
            request_id: row.id,
            camera_id,
            mode,
            label: row.label,
            started_at: row.started_at,
            ends_at: row.ends_at,
            pre_seconds,
            policy_pre_seconds: cam.policy.motion_pre_seconds,
            policy_post_seconds: cam.policy.motion_post_seconds,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(seconds: i64, label: Option<&str>, pre: Option<i64>) -> RecordNowRequest {
        RecordNowRequest {
            seconds,
            label: label.map(str::to_owned),
            pre_seconds: pre,
        }
    }

    #[test]
    fn validate_accepts_a_plain_request_and_trims_the_label() {
        let (s, p, l) = validate(&body(30, Some("  gate  "), None)).unwrap();
        assert_eq!((s, p), (30, 0));
        assert_eq!(l.as_deref(), Some("gate"));
        let (_, _, l) = validate(&body(30, Some("   "), Some(5))).unwrap();
        assert_eq!(l, None);
    }

    #[test]
    fn validate_rejects_out_of_range_values() {
        assert!(validate(&body(0, None, None)).is_err());
        assert!(validate(&body(MAX_SECONDS + 1, None, None)).is_err());
        assert!(validate(&body(10, None, Some(-1))).is_err());
        assert!(validate(&body(10, None, Some(MAX_PRE_SECONDS + 1))).is_err());
        assert!(validate(&body(10, Some(&"x".repeat(MAX_LABEL_CHARS + 1)), None)).is_err());
        assert!(validate(&body(10, Some("a\nb"), None)).is_err());
        assert!(validate(&body(
            MAX_SECONDS,
            Some(&"y".repeat(MAX_LABEL_CHARS)),
            Some(MAX_PRE_SECONDS)
        ))
        .is_ok());
    }
}
