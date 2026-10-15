-- 0083_motion_api_source.sql — the `api` motion source: externally triggered
-- recording ("record now") for integrators, plus the `record_now` capability.
--
-- A fourth ADDITIVE motion source beside pixel / Frigate / Home Assistant
-- (migration 0049): `cameras.motion_api_enabled`. When enabled, the recorder runs
-- an `api` source loop for the camera that turns rows of `motion_api_requests`
-- into MotionSignal START/STOP edges, so a Motion-mode camera persists its RAM
-- pre-roll and records for the requested window exactly as it would for a pixel
-- or sensor trigger. `NOT NULL DEFAULT false`: NO existing camera changes
-- behaviour on upgrade. See docs/DECISIONS.md (2026-10-15, api motion source) and
-- docs/MOTION-RECORDING.md §9.
--
-- `motion_api_requests` is the DURABLE request log the api writes and the
-- recorder reads. The api also raises `pg_notify('crumb_motion_api', …)` on every
-- insert so the recorder reacts within milliseconds, but the table is the source
-- of truth: a recorder that was down (or whose LISTEN connection dropped) reloads
-- every request whose window is still open when its source loop (re)starts, so a
-- request is never lost to a missed notification. Rows cascade with the camera.
-- `ends_at` is denormalised (started_at + seconds) so the catch-up query is a
-- single indexed range scan.
--
-- `record_now` is a role capability (jsonb, migration 0028 pattern): the Rust
-- `Capabilities` struct reads a missing key as its serde default (FALSE = deny),
-- so this UPDATE changes NO effective permission; it only makes the key explicit
-- in stored rows (console checkbox reflects stored state) and records when the
-- capability appeared. Admin roles bypass capabilities and are left untouched.
--
-- Idempotent and additive; safe to re-run.

ALTER TABLE cameras
    ADD COLUMN IF NOT EXISTS motion_api_enabled boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN cameras.motion_api_enabled IS
    'Additive motion source (migration 0083): when true the recorder records this '
    'camera on POST /cameras/:id/record-now requests (motion_api_requests rows).';

CREATE TABLE IF NOT EXISTS motion_api_requests (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    camera_id     uuid NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
    -- Who asked (users.id); nullable, no FK so user deletion never orphans a row.
    requested_by  uuid,
    -- Optional free-text label surfaced as the `events` row label (NULL = "record").
    label         text,
    -- Extra pre-roll the caller asked for, on top of the policy's motion_pre_seconds
    -- (bounded by what the RAM ring still holds). Informational for the recorder:
    -- it is already folded into started_at by the api.
    pre_seconds   integer NOT NULL DEFAULT 0,
    -- The recording window the recorder must keep: [started_at, ends_at].
    started_at    timestamptz NOT NULL,
    ends_at       timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- The recorder's catch-up query: open windows for one camera.
CREATE INDEX IF NOT EXISTS motion_api_requests_camera_ends
    ON motion_api_requests (camera_id, ends_at);

UPDATE roles
SET capabilities = capabilities || '{"record_now": false}'::jsonb
WHERE NOT is_admin
  AND NOT (capabilities ? 'record_now');

-- Re-declare v_camera_effective_policy to expose the new boolean. Per the CREATE
-- OR REPLACE VIEW rule every existing column keeps its name AND order and the new
-- column is APPENDED at the end (after c_ptz_control_enabled, migration 0061).
CREATE OR REPLACE VIEW v_camera_effective_policy AS
SELECT
    c.id              AS c_id,
    c.name            AS c_name,
    c.enabled         AS c_enabled,
    c.go2rtc_name     AS c_go2rtc_name,
    c.main_url        AS c_main_url,
    c.sub_url         AS c_sub_url,
    c.source_url      AS c_source_url,
    c.source_sub_url  AS c_source_sub_url,
    c.policy_id       AS c_policy_id,
    m.group_id        AS c_group_id,
    c.motion_mask     AS c_motion_mask,
    c.onvif_motion    AS c_onvif_motion,
    c.motion_source     AS c_motion_source,
    c.motion_algorithm  AS c_motion_algorithm,
    c.camera_type       AS c_camera_type,
    c.icon              AS c_icon,
    c.motion_grid_cols  AS c_motion_grid_cols,
    c.motion_grid_rows  AS c_motion_grid_rows,
    c.created_at        AS c_created_at,
    c.served_by         AS c_served_by,
    c.source_camera_name AS c_source_camera_name,
    c.onvif_host        AS c_onvif_host,
    c.onvif_port        AS c_onvif_port,
    c.onvif_user        AS c_onvif_user,
    c.onvif_password    AS c_onvif_password,
    p.id                      AS p_id,
    p.name                    AS p_name,
    p.is_default              AS p_is_default,
    p.mode                    AS p_mode,
    p.live_storage_id         AS p_live_storage_id,
    p.live_retention_hours    AS p_live_retention_hours,
    p.archive_enabled         AS p_archive_enabled,
    p.archive_storage_id      AS p_archive_storage_id,
    p.archive_schedule        AS p_archive_schedule,
    p.archive_retention_hours AS p_archive_retention_hours,
    p.live_max_bytes          AS p_live_max_bytes,
    p.archive_max_bytes       AS p_archive_max_bytes,
    p.live_min_free_pct          AS p_live_min_free_pct,
    p.live_min_free_bytes        AS p_live_min_free_bytes,
    p.live_spill_low_water_bytes AS p_live_spill_low_water_bytes,
    p.motion_pre_seconds      AS p_motion_pre_seconds,
    p.motion_post_seconds     AS p_motion_post_seconds,
    p.motion_sensitivity      AS p_motion_sensitivity,
    p.motion_threshold        AS p_motion_threshold,
    p.motion_keyframes_only   AS p_motion_keyframes_only,
    p.record_stream           AS p_record_stream,
    p.record_audio            AS p_record_audio,
    p.max_retention_days      AS p_max_retention_days,
    c.motion_pixel_enabled    AS c_motion_pixel_enabled,
    c.motion_frigate_enabled  AS c_motion_frigate_enabled,
    c.motion_ha_enabled       AS c_motion_ha_enabled,
    c.ptz_control_enabled     AS c_ptz_control_enabled,
    c.motion_api_enabled      AS c_motion_api_enabled
FROM cameras c
LEFT JOIN camera_group_members m ON m.camera_id = c.id
LEFT JOIN camera_groups g ON g.id = m.group_id
JOIN recording_policies p ON p.id = COALESCE(
    c.policy_id,
    g.policy_id,
    (SELECT id FROM recording_policies WHERE is_default LIMIT 1)
);
