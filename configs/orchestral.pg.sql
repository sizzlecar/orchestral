-- =========================================================
-- Orchestral PostgreSQL DDL (No Foreign Keys)
-- Principles:
-- - No FK constraints to avoid cross-system blocking.
-- - Eventual consistency is enforced by application code.
-- - Tables are linked by logical IDs only.
-- =========================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------
-- 1) Thread
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestral_thread (
  id           TEXT PRIMARY KEY,
  scope        TEXT NOT NULL,
  title        TEXT,
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orchestral_thread_scope
  ON orchestral_thread(scope);

CREATE INDEX IF NOT EXISTS idx_orchestral_thread_updated
  ON orchestral_thread(updated_at DESC);

-- ---------------------------------------------------------
-- 2) Thread Summary (incremental anchor by last_event_id)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestral_thread_summary (
  thread_id        TEXT PRIMARY KEY,
  summary_text     TEXT NOT NULL,
  model            TEXT,
  version          TEXT,
  last_event_id    BIGINT NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_orchestral_thread_summary_updated
  ON orchestral_thread_summary(updated_at DESC);

-- ---------------------------------------------------------
-- 3) Task
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestral_task (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  thread_id              TEXT,
  intent_id              TEXT,
  intent_content         TEXT NOT NULL,
  intent_context         JSONB NOT NULL DEFAULT '{}'::jsonb,
  state                  TEXT NOT NULL,
  state_payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
  plan                   JSONB,
  completed_step_ids     TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
  working_set_snapshot   JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orchestral_task_thread_updated
  ON orchestral_task(thread_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_orchestral_task_state_updated
  ON orchestral_task(state, updated_at DESC);

-- ---------------------------------------------------------
-- 4) File (managed file metadata catalog)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestral_file (
  id               TEXT PRIMARY KEY,
  backend          TEXT NOT NULL,    -- local / s3 / custom:*
  local_path       TEXT,
  bucket           TEXT,
  object_key       TEXT,
  file_name        TEXT,
  mime_type        TEXT,
  byte_size        BIGINT NOT NULL,
  checksum_sha256  TEXT,
  status           TEXT NOT NULL,    -- active / deleted
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_orchestral_file_status_updated
  ON orchestral_file(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_orchestral_file_created
  ON orchestral_file(created_at DESC);

-- ---------------------------------------------------------
-- 5) Reference (metadata index only; file assets point to file_id)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestral_reference (
  id                   TEXT PRIMARY KEY,
  thread_id            TEXT NOT NULL,
  interaction_id       TEXT,
  task_id              UUID,
  step_id              TEXT,
  file_id              TEXT,
  ref_type             TEXT NOT NULL,
  content              JSONB NOT NULL DEFAULT '{}'::jsonb,
  mime_type            TEXT,
  file_name            TEXT,
  byte_size            BIGINT,
  derived_from         TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
  tags                 TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
  metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedding_backend    TEXT,
  embedding_collection TEXT,
  embedding_key        TEXT,
  embedding_model      TEXT,
  embedding_status     TEXT,
  embedding_version    TEXT,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orchestral_reference_thread_created
  ON orchestral_reference(thread_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orchestral_reference_thread_type_created
  ON orchestral_reference(thread_id, ref_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orchestral_reference_task_step
  ON orchestral_reference(task_id, step_id);

CREATE INDEX IF NOT EXISTS idx_orchestral_reference_file_id
  ON orchestral_reference(file_id);

CREATE INDEX IF NOT EXISTS gin_orchestral_reference_tags
  ON orchestral_reference USING GIN (tags);

CREATE INDEX IF NOT EXISTS gin_orchestral_reference_metadata
  ON orchestral_reference USING GIN (metadata);

-- ---------------------------------------------------------
-- 6) Event (append-only fact stream)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS orchestral_event (
  id              BIGSERIAL PRIMARY KEY,
  thread_id       TEXT NOT NULL,
  interaction_id  TEXT,
  event_type      TEXT NOT NULL,
  payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
  reference_id    TEXT,
  task_id         UUID,
  step_id         TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orchestral_event_thread_created
  ON orchestral_event(thread_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_orchestral_event_thread_type_created
  ON orchestral_event(thread_id, event_type, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_orchestral_event_interaction
  ON orchestral_event(thread_id, interaction_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_orchestral_event_reference
  ON orchestral_event(reference_id);
