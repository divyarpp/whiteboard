-- ============================================================
--  Whiteboard schema — Phase 4 (core save/load tables)
--  Run this once in Supabase: SQL Editor -> New query -> paste -> Run.
--  The users / participants / operation_log tables arrive with
--  their own phases (auth in Phase 7, realtime log in Phase 5/6).
-- ============================================================

create extension if not exists pgcrypto;   -- provides gen_random_uuid()

create table if not exists sessions (
  session_id    uuid primary key default gen_random_uuid(),
  session_code  text unique not null,        -- short code people type to join
  title         text not null default 'Untitled whiteboard',
  owner_user_id uuid,                          -- linked to a real user in Phase 7
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

create table if not exists pages (
  page_id     uuid primary key default gen_random_uuid(),
  session_id  uuid not null references sessions(session_id) on delete cascade,
  page_number int  not null,
  width       int  not null default 794,
  height      int  not null default 1123,
  background  text not null default 'white',
  created_at  timestamptz not null default now()
);
create index if not exists idx_pages_session on pages(session_id);

create table if not exists whiteboard_objects (
  object_id   text primary key,               -- the client-generated 'obj_...' id
  session_id  uuid not null references sessions(session_id) on delete cascade,
  page_id     uuid not null references pages(page_id) on delete cascade,
  object_type text not null,                   -- pen, highlighter, line, rect, ellipse, arrow, text, image
  data_json   jsonb not null,                  -- points / coords / text / color / width, etc.
  created_by  uuid,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  is_deleted  boolean not null default false
);
create index if not exists idx_objects_session on whiteboard_objects(session_id);
create index if not exists idx_objects_page on whiteboard_objects(page_id);