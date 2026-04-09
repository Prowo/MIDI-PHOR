-- ================================
-- Base DDL: tables + indexes
-- Works in DuckDB and Postgres
-- ================================

BEGIN;

-- ---------- Core song/time backbone ----------
CREATE TABLE IF NOT EXISTS songs (
  song_id      TEXT PRIMARY KEY,
  title        TEXT,
  ppq          INTEGER,          -- pulses per quarter
  duration_sec DOUBLE
);

CREATE TABLE IF NOT EXISTS tempo_changes (
  song_id TEXT,
  t_sec   DOUBLE,
  qpm     DOUBLE,                 -- quarters per minute
  PRIMARY KEY (song_id, t_sec)
);

CREATE TABLE IF NOT EXISTS timesig_changes (
  song_id TEXT,
  t_sec   DOUBLE,
  num     INTEGER,
  den     INTEGER,
  PRIMARY KEY (song_id, t_sec)
);

CREATE TABLE IF NOT EXISTS bars (
  song_id   TEXT,
  bar       INTEGER,              -- 1-based
  start_sec DOUBLE,
  end_sec   DOUBLE,
  num       INTEGER,              -- local time signature
  den       INTEGER,
  qpm       DOUBLE,               -- local tempo
  PRIMARY KEY (song_id, bar)
);

CREATE INDEX IF NOT EXISTS idx_bars_song ON bars(song_id);
CREATE INDEX IF NOT EXISTS idx_bars_song_bar ON bars(song_id, bar);

CREATE TABLE IF NOT EXISTS sections (
  song_id     TEXT,
  section_id  TEXT,
  type        TEXT,               -- intro|verse|chorus|bridge|outro|other
  start_bar   INTEGER,
  end_bar     INTEGER,            -- inclusive
  start_sec   DOUBLE,
  end_sec     DOUBLE,
  source      TEXT,               -- symbolic|audio|merged
  confidence  DOUBLE,
  PRIMARY KEY (song_id, section_id)
);

CREATE INDEX IF NOT EXISTS idx_sections_song ON sections(song_id);
CREATE INDEX IF NOT EXISTS idx_sections_song_range ON sections(song_id, start_bar, end_bar);

-- ---------- Symbolic layer ----------
CREATE TABLE IF NOT EXISTS tracks (
  song_id    TEXT,
  track_id   TEXT,
  name       TEXT,
  gm_program INTEGER,
  role       TEXT,                -- melody|pad|bass|perc|comp|lead|fx|other
  PRIMARY KEY (song_id, track_id)
);

CREATE INDEX IF NOT EXISTS idx_tracks_song ON tracks(song_id);

CREATE TABLE IF NOT EXISTS notes (
  song_id     TEXT,
  note_id     TEXT,
  track_id    TEXT,
  pitch       INTEGER,
  velocity    INTEGER,
  onset_sec   DOUBLE,
  offset_sec  DOUBLE,
  onset_bar   INTEGER,
  onset_beat  DOUBLE,
  dur_beats   DOUBLE,
  PRIMARY KEY (song_id, note_id)
);

CREATE INDEX IF NOT EXISTS idx_notes_song_bar ON notes(song_id, onset_bar);
CREATE INDEX IF NOT EXISTS idx_notes_song_track ON notes(song_id, track_id);

CREATE TABLE IF NOT EXISTS key_changes (
  song_id  TEXT,
  at_bar   INTEGER,
  at_beat  DOUBLE,
  key      TEXT,                  -- e.g., "C:maj", "A:min"
  confidence DOUBLE,
  PRIMARY KEY (song_id, at_bar, at_beat)
);

-- Optional: inferred key regions (e.g., modulation tracking) that should not affect
-- Roman-numeral labeling, which typically assumes a single reference key.
CREATE TABLE IF NOT EXISTS key_regions (
  song_id     TEXT,
  start_bar   INTEGER,
  end_bar     INTEGER,
  key         TEXT,               -- e.g., "C:maj", "A:min"
  confidence  DOUBLE,
  PRIMARY KEY (song_id, start_bar, end_bar, key)
);

CREATE INDEX IF NOT EXISTS idx_key_regions_song ON key_regions(song_id);

CREATE TABLE IF NOT EXISTS chords (
  song_id     TEXT,
  chord_id    TEXT,
  onset_bar   INTEGER,
  onset_beat  DOUBLE,
  dur_beats   DOUBLE,
  name        TEXT,               -- e.g., "C:maj7/G"
  rn          TEXT,               -- Roman numeral (local key)
  root_pc     INTEGER,
  quality     TEXT,               -- maj|min|dim|aug|dom|sus|other
  pcset       TEXT,               -- "{0,4,7,11}"
  section_id  TEXT,               -- nullable
  PRIMARY KEY (song_id, chord_id)
);

CREATE INDEX IF NOT EXISTS idx_chords_song_bar ON chords(song_id, onset_bar);

CREATE TABLE IF NOT EXISTS bar_metrics (
  song_id            TEXT,
  bar                INTEGER,
  density            DOUBLE,      -- note-ons per bar (normalized)
  polyphony          DOUBLE,      -- avg concurrent pitches
  backbeat_strength  DOUBLE,      -- snare@2/4 / total drums (4/4 only)
  syncopation        DOUBLE,      -- off-beat onset ratio
  velocity_mean      DOUBLE,
  velocity_std       DOUBLE,
  PRIMARY KEY (song_id, bar)
);

-- Motifs (symbolic hook patterns)
CREATE TABLE IF NOT EXISTS motifs (
  song_id     TEXT,
  motif_id    TEXT,
  pattern     TEXT,               -- e.g., "i+2 d2 i+2 d2 i-1 d4" (example)
  occurrences TEXT,               -- JSON string: e.g., "[{\"bar\":5},{\"bar\":13}]"
  support     INTEGER,            -- count of occurrences
  PRIMARY KEY (song_id, motif_id)
);

-- ---------- Time-series layer ----------
-- Keep frame alignment stable: prefer integer ms and/or frame_idx
CREATE TABLE IF NOT EXISTS ts_frame (
  song_id    TEXT,
  feature    TEXT,                -- "rms","novelty","chroma_c",...
  frame_idx  INTEGER,             -- optional; index into frames for this feature
  t_ms       INTEGER,             -- integer milliseconds for robust joins
  t_sec      DOUBLE,              -- convenience (do not key on this)
  value      DOUBLE,
  PRIMARY KEY (song_id, feature, t_ms)
);

CREATE INDEX IF NOT EXISTS idx_ts_frame_song_feat ON ts_frame(song_id, feature);
CREATE INDEX IF NOT EXISTS idx_ts_frame_song_ms   ON ts_frame(song_id, t_ms);

CREATE TABLE IF NOT EXISTS ts_bar (
  song_id  TEXT,
  bar      INTEGER,
  feature  TEXT,                  -- "energy_bar","novelty_bar","density",...
  value    DOUBLE,
  PRIMARY KEY (song_id, bar, feature)
);

CREATE INDEX IF NOT EXISTS idx_ts_bar_song ON ts_bar(song_id);
CREATE INDEX IF NOT EXISTS idx_ts_bar_song_bar ON ts_bar(song_id, bar);

-- Section-level tags (audio-required predictions or curated labels)
CREATE TABLE IF NOT EXISTS tags_section (
  song_id    TEXT,
  section_id TEXT,
  tag_type   TEXT,                -- "genre"|"mood"|"timbre"|...
  tag        TEXT,                -- e.g., "pop","energetic","bright synth"
  confidence DOUBLE,
  PRIMARY KEY (song_id, section_id, tag_type, tag)
);

CREATE INDEX IF NOT EXISTS idx_tags_section_song ON tags_section(song_id);

-- Optional: events derived from time series (boundaries, drops, entries)
CREATE TABLE IF NOT EXISTS events (
  song_id    TEXT,
  bar        INTEGER,
  event_type TEXT,                -- 'BOUNDARY'|'CLIMAX'|'DROP'|'ENTRY_DRUMS'|...
  detail     TEXT,                -- free text or JSON
  strength   DOUBLE,
  PRIMARY KEY (song_id, bar, event_type)
);

-- ---------- Orchestration graph ----------
CREATE TABLE IF NOT EXISTS graph_nodes (
  song_id   TEXT,
  node_id   TEXT,                 -- often == track_id or role id
  node_type TEXT,                 -- "track"|"role"|"family"
  track_id  TEXT,                 -- nullable if role/family node
  role      TEXT,                 -- melody|pad|bass|perc|comp|lead|other
  family    TEXT,                 -- strings|keys|guitars|synth|drums|vox|etc.
  PRIMARY KEY (song_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_gnodes_song ON graph_nodes(song_id);

CREATE TABLE IF NOT EXISTS graph_edges (
  song_id     TEXT,
  edge_id     TEXT,
  src_node_id TEXT,
  dst_node_id TEXT,
  rel_type    TEXT,               -- cooccur|supports|doubles|call_response|rhythmic_lock
  strength    DOUBLE,             -- 0..1 (normalized)
  PRIMARY KEY (song_id, edge_id)
);

CREATE INDEX IF NOT EXISTS idx_gedges_song ON graph_edges(song_id);
CREATE INDEX IF NOT EXISTS idx_gedges_song_nodes ON graph_edges(song_id, src_node_id, dst_node_id);

CREATE TABLE IF NOT EXISTS edge_evidence (
  song_id    TEXT,
  edge_id    TEXT,
  section_id TEXT,                -- nullable
  start_bar  INTEGER,
  end_bar    INTEGER,
  events     INTEGER,             -- e.g., # co-onsets/doublings
  confidence DOUBLE,
  PRIMARY KEY (song_id, edge_id, start_bar, end_bar)
);

CREATE TABLE IF NOT EXISTS node_activity (
  song_id     TEXT,
  node_id     TEXT,
  start_bar   INTEGER,
  end_bar     INTEGER,
  active_ratio DOUBLE,
  PRIMARY KEY (song_id, node_id, start_bar, end_bar)
);

-- ---------- Feature registry (metadata about features) ----------
CREATE TABLE IF NOT EXISTS features_meta (
  feature     TEXT PRIMARY KEY,
  domain      TEXT,               -- "audio"|"symbolic"
  hop_sec     DOUBLE,             -- if frame-based
  win_sec     DOUBLE,
  description TEXT
);

COMMIT;
