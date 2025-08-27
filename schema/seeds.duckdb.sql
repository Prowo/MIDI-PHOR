-- ================================
-- DuckDB seed data (idempotent)
-- ================================

-- Insert rows only if feature not present.
-- DuckDB tip: emulate "DO NOTHING" with anti-join.

INSERT INTO features_meta
SELECT * FROM (
  SELECT 'rms'            AS feature, 'audio' AS domain, 0.0232 AS hop_sec, 0.0464 AS win_sec, 'librosa RMS' AS description
  UNION ALL SELECT 'novelty','audio',0.0232,0.0928,'spectral flux / novelty function'
  UNION ALL SELECT 'chroma_c','audio',0.0464,0.1856,'constant-Q chroma (mean)'
  UNION ALL SELECT 'energy_bar','audio',NULL,NULL,'bar-mean RMS'
  UNION ALL SELECT 'brightness_bar','audio',NULL,NULL,'bar-mean spectral centroid'
  UNION ALL SELECT 'percussive_ratio_bar','audio',NULL,NULL,'HPSS percussive/(harmonic+percussive)'
  UNION ALL SELECT 'density','symbolic',NULL,NULL,'note-ons per bar (normalized)'
  UNION ALL SELECT 'polyphony','symbolic',NULL,NULL,'avg concurrent pitches'
  UNION ALL SELECT 'backbeat_strength','symbolic',NULL,NULL,'snare@2/4 ratio (4/4)'
  UNION ALL SELECT 'syncopation','symbolic',NULL,NULL,'off-beat onset ratio'
  UNION ALL SELECT 'harmonic_rhythm','symbolic',NULL,NULL,'chord changes per bar'
  UNION ALL SELECT 'key_clarity','mixed',NULL,NULL,'tonal clarity / chroma entropy inverse'
  UNION ALL SELECT 'active_tracks','symbolic',NULL,NULL,'# active tracks per bar'
) AS s
WHERE NOT EXISTS (SELECT 1 FROM features_meta f WHERE f.feature = s.feature);
