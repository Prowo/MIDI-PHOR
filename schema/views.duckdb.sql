-- ================================
-- Views for DuckDB
-- ================================

-- Map frames to bars once; reuse everywhere
CREATE OR REPLACE VIEW v_frames_with_bars AS
SELECT
  f.song_id,
  f.feature,
  f.t_ms,
  f.t_sec,
  f.value,
  b.bar
FROM ts_frame f
JOIN bars b
  ON f.song_id = b.song_id
 AND f.t_ms BETWEEN CAST(b.start_sec * 1000 AS INTEGER)
                AND CAST(b.end_sec   * 1000 AS INTEGER);

-- Convenience: per-section bar list
CREATE OR REPLACE VIEW v_bars_in_sections AS
SELECT
  s.song_id,
  s.section_id,
  s.type,
  b.bar
FROM sections s
JOIN bars b
  ON b.song_id = s.song_id
 AND b.bar BETWEEN s.start_bar AND s.end_bar;

-- Wide per-bar view (pivot). Adjust the feature list as you add more.
CREATE OR REPLACE VIEW v_ts_bar_wide AS
SELECT *
FROM ts_bar
PIVOT (avg(value) FOR feature IN
  ('energy_bar',
   'energy_bar_z',
   'energy_bar_delta',
   'novelty_bar',
   'density',
   'polyphony',
   'backbeat_strength',
   'syncopation',
   'harmonic_rhythm',
   'key_clarity',
   'active_tracks',
   'active_drums',
   'active_bass',
   'active_pad',
   'active_melody'));
