# extractors/audio.py
from __future__ import annotations
import os, math, json
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import numpy as np
import duckdb
import librosa
import soundfile as sf
import pretty_midi

from utils.ids import deterministic_id

@dataclass
class AudioConfig:
    sr: int = 44100
    hop_length: int = 1024
    n_fft: int = 2048
    novelty_win: int = 3
    boundary_z: float = 1.0
    drop_delta_thresh: float = -0.8
    repeat_sim_thresh: float = 0.85
    soundfont_path: Optional[str] = None  # .sf2 for pretty_midi.fluidsynth
    render_gain_db: float = 0.0

# ---------- Rendering ----------

def render_midi_to_wav(midi_path: str, wav_out: str, cfg: AudioConfig) -> str:
    pm = pretty_midi.PrettyMIDI(midi_path)
    if cfg.soundfont_path:
        audio = pm.fluidsynth(fs=cfg.sr, sf2_path=cfg.soundfont_path)
    else:
        # fallback synth (simple): sum sine tones — not great, but OK for features
        audio = pm.fluidsynth(fs=cfg.sr)
    # normalize + gain
    if np.max(np.abs(audio)) > 0:
        audio = audio / np.max(np.abs(audio))
    if cfg.render_gain_db != 0.0:
        audio = audio * (10 ** (cfg.render_gain_db / 20.0))
    sf.write(wav_out, audio, cfg.sr)
    return wav_out

# ---------- Frames → DB ----------

def _store_frames(con: duckdb.DuckDBPyConnection, song_id: str, feature: str, t_ms: np.ndarray, values: np.ndarray) -> None:
    if len(values) == 0: return
    con.register("tmp_frames", 
                 [(song_id, feature, int(ms), float(ms)/1000.0, float(v)) for ms, v in zip(t_ms.astype(int), values)])
    con.execute("""
        INSERT OR REPLACE INTO ts_frame (song_id, feature, t_ms, t_sec, value)
        SELECT * FROM tmp_frames
    """)
    con.unregister("tmp_frames")

def _aggregate_frames_to_bars(con: duckdb.DuckDBPyConnection, song_id: str, feature: str, out_feature: str, agg: str = "AVG") -> None:
    con.execute(f"""
        INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value)
        SELECT f.song_id, b.bar, '{out_feature}' AS feature, {agg}(f.value) AS value
        FROM ts_frame f
        JOIN bars b ON b.song_id=f.song_id
                   AND f.t_ms BETWEEN CAST(b.start_sec*1000 AS INTEGER) AND CAST(b.end_sec*1000 AS INTEGER)
        WHERE f.song_id=? AND f.feature=?
        GROUP BY f.song_id, b.bar
    """, [song_id, feature])

def _compute_z_and_delta(con: duckdb.DuckDBPyConnection, song_id: str, base_feature: str) -> None:
    # z-score
    con.execute("""
        WITH x AS (
          SELECT song_id, bar, value,
                 AVG(value) OVER (PARTITION BY song_id) mu,
                 STDDEV_SAMP(value) OVER (PARTITION BY song_id) sigma
          FROM ts_bar WHERE song_id=? AND feature=?
        )
        INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value)
        SELECT song_id, bar, ? || '_z', CASE WHEN sigma>0 THEN (value-mu)/sigma ELSE 0 END FROM x
    """, [song_id, base_feature, base_feature])
    # delta
    con.execute("""
        WITH x AS (
          SELECT song_id, bar, value,
                 value - LAG(value) OVER (PARTITION BY song_id ORDER BY bar) AS d
          FROM ts_bar WHERE song_id=? AND feature=?
        )
        INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value)
        SELECT song_id, bar, ? || '_delta', COALESCE(d,0) FROM x
    """, [song_id, base_feature, base_feature])

# ---------- Feature extraction ----------

def _audio_features_to_db(con: duckdb.DuckDBPyConnection, song_id: str, wav_path: str, cfg: AudioConfig) -> None:
    y, sr = librosa.load(wav_path, sr=cfg.sr, mono=True)
    hop = cfg.hop_length

    # frames time (ms)
    t_ms = (librosa.frames_to_time(np.arange(0, 1 + len(y) // hop), sr=sr, hop_length=hop) * 1000.0)
    t_ms = t_ms[:max(1, len(t_ms)-1)]  # align sizes

    # RMS → energy
    rms = librosa.feature.rms(y=y, frame_length=cfg.n_fft, hop_length=hop, center=True).flatten()[:len(t_ms)]
    _store_frames(con, song_id, "rms", t_ms, rms)
    _aggregate_frames_to_bars(con, song_id, "rms", "energy_bar")
    _compute_z_and_delta(con, song_id, "energy_bar")

    # Spectral centroid → brightness
    sc = librosa.feature.spectral_centroid(y=y, sr=sr, n_fft=cfg.n_fft, hop_length=hop).flatten()[:len(t_ms)]
    _store_frames(con, song_id, "centroid", t_ms, sc)
    _aggregate_frames_to_bars(con, song_id, "centroid", "brightness_bar")
    _compute_z_and_delta(con, song_id, "brightness_bar")

    # Zero-crossing rate (texture/brightness proxy)
    zcr = librosa.feature.zero_crossing_rate(y=y, frame_length=cfg.n_fft, hop_length=hop).flatten()[:len(t_ms)]
    _store_frames(con, song_id, "zcr", t_ms, zcr)
    _aggregate_frames_to_bars(con, song_id, "zcr", "zcr_bar")
    _compute_z_and_delta(con, song_id, "zcr_bar")

    # Spectral rolloff / bandwidth / flatness
    roll = librosa.feature.spectral_rolloff(y=y, sr=sr, hop_length=hop).flatten()[:len(t_ms)]
    bw = librosa.feature.spectral_bandwidth(y=y, sr=sr, hop_length=hop).flatten()[:len(t_ms)]
    flat = librosa.feature.spectral_flatness(y=y, n_fft=cfg.n_fft, hop_length=hop).flatten()[:len(t_ms)]
    _store_frames(con, song_id, "rolloff", t_ms, roll)
    _store_frames(con, song_id, "bandwidth", t_ms, bw)
    _store_frames(con, song_id, "flatness", t_ms, flat)
    _aggregate_frames_to_bars(con, song_id, "rolloff", "rolloff_bar")
    _aggregate_frames_to_bars(con, song_id, "bandwidth", "bandwidth_bar")
    _aggregate_frames_to_bars(con, song_id, "flatness", "flatness_bar")
    _compute_z_and_delta(con, song_id, "rolloff_bar")
    _compute_z_and_delta(con, song_id, "bandwidth_bar")
    _compute_z_and_delta(con, song_id, "flatness_bar")

    # Spectral flux / novelty
    S = np.abs(librosa.stft(y, n_fft=cfg.n_fft, hop_length=hop)) + 1e-8
    nov = np.sqrt(np.sum(np.diff(S, axis=1).clip(min=0.0) ** 2, axis=0))
    nov = np.concatenate([[0.0], nov])[:len(t_ms)]
    _store_frames(con, song_id, "novelty", t_ms, nov)
    _aggregate_frames_to_bars(con, song_id, "novelty", "novelty_bar")
    _compute_z_and_delta(con, song_id, "novelty_bar")

    # Onset strength envelope (librosa)
    onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=hop)
    onset_env = onset_env[:len(t_ms)]
    _store_frames(con, song_id, "onset_strength", t_ms, onset_env)
    _aggregate_frames_to_bars(con, song_id, "onset_strength", "onset_strength_bar")
    _compute_z_and_delta(con, song_id, "onset_strength_bar")

    # Tempogram → peak strength per frame
    try:
        tpg = librosa.feature.tempogram(onset_envelope=onset_env, sr=sr, hop_length=hop)
        tpg = tpg[:, :len(t_ms)]
        tpg_peak = np.max(tpg, axis=0)
        _store_frames(con, song_id, "tempogram_peak", t_ms, tpg_peak)
        _aggregate_frames_to_bars(con, song_id, "tempogram_peak", "tempogram_peak_bar")
        _compute_z_and_delta(con, song_id, "tempogram_peak_bar")
    except Exception:
        pass

    # Instantaneous tempo curve (BPM) using onset envelope
    try:
        tempo_curve = librosa.beat.tempo(sr=sr, hop_length=hop, onset_envelope=onset_env, aggregate=None)
        tempo_curve = tempo_curve[:len(t_ms)]
        _store_frames(con, song_id, "tempo_inst", t_ms, tempo_curve)
        _aggregate_frames_to_bars(con, song_id, "tempo_inst", "tempo_bar")
        _compute_z_and_delta(con, song_id, "tempo_bar")
    except Exception:
        pass

    # Predominant local pulse (PLP) strength
    try:
        plp = librosa.beat.plp(y=y, sr=sr, hop_length=hop)
        plp = plp[:len(t_ms)]
        _store_frames(con, song_id, "plp", t_ms, plp)
        _aggregate_frames_to_bars(con, song_id, "plp", "pulse_strength_bar")
        _compute_z_and_delta(con, song_id, "pulse_strength_bar")
    except Exception:
        pass

    # Chroma CQT (12 bins) for repeat/section similarity
    chroma = librosa.feature.chroma_cqt(y=y, sr=sr, hop_length=hop, n_chroma=12, bins_per_octave=36)
    chroma = chroma[:, :len(t_ms)]
    for k in range(12):
        _store_frames(con, song_id, f"chroma_c_{k}", t_ms, chroma[k])

    # Aggregate chroma to bars (mean per bar per bin)
    for k in range(12):
        _aggregate_frames_to_bars(con, song_id, f"chroma_c_{k}", f"chroma_c_{k}_bar")

    # MFCCs (13) → bar averages
    try:
        mfcc = librosa.feature.mfcc(y=y, sr=sr, hop_length=hop, n_mfcc=13)
        mfcc = mfcc[:, :len(t_ms)]
        for k in range(mfcc.shape[0]):
            _store_frames(con, song_id, f"mfcc_{k}", t_ms, mfcc[k])
            _aggregate_frames_to_bars(con, song_id, f"mfcc_{k}", f"mfcc_{k}_bar")
        # Optionally z/delta for first few
        for base in [f"mfcc_{k}_bar" for k in range(min(5, mfcc.shape[0]))]:
            _compute_z_and_delta(con, song_id, base)
    except Exception:
        pass

def _ensure_symbolic_family_counts(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    """
    Compute active family/role counts per bar from notes+tracks and write to ts_bar:
    active_tracks, active_drums, active_bass, active_pad, active_melody
    Uses tracks.role when available; falls back to GM program/is_drum heuristics.
    """
    # Add a temp view 'track_family'
    con.execute("""
        CREATE TEMP VIEW IF NOT EXISTS __track_family AS
        SELECT
          t.song_id,
          t.track_id,
          CASE
            WHEN t.role IS NOT NULL THEN
                CASE
                  WHEN LOWER(t.role) LIKE '%perc%' THEN 'drums'
                  WHEN LOWER(t.role) LIKE '%bass%' THEN 'bass'
                  WHEN LOWER(t.role) LIKE '%melod%' OR LOWER(t.role) LIKE '%lead%' THEN 'melody'
                  WHEN LOWER(t.role) LIKE '%pad%'  OR LOWER(t.role) LIKE '%comp%' THEN 'pad'
                  ELSE 'other'
                END
            ELSE
                CASE
                  WHEN t.gm_program BETWEEN 32 AND 39 THEN 'bass'      -- GM bass family (approx)
                  WHEN t.gm_program BETWEEN 88 AND 95 THEN 'pad'       -- GM pad 1..8
                  WHEN t.gm_program BETWEEN 80 AND 87 THEN 'melody'    -- GM lead 1..8
                  WHEN t.gm_program BETWEEN 115 AND 119 THEN 'drums'   -- extra perc FX guard
                  ELSE 'other'
                END
          END AS family
        FROM tracks t
        WHERE t.song_id=?
    """, [song_id])

    # active_tracks
    con.execute("""
        INSERT OR REPLACE INTO ts_bar
        SELECT n.song_id AS song_id, n.onset_bar AS bar, 'active_tracks' AS feature,
               CAST(COUNT(DISTINCT n.track_id) AS DOUBLE) AS value
        FROM notes n
        WHERE n.song_id=?
        GROUP BY n.song_id, n.onset_bar
    """, [song_id])

    # per-family counts
    for fam, feat in [("drums","active_drums"),("bass","active_bass"),
                      ("pad","active_pad"),("melody","active_melody")]:
        con.execute(f"""
            INSERT OR REPLACE INTO ts_bar
            SELECT n.song_id, n.onset_bar, '{feat}' AS feature,
                   CAST(COUNT(DISTINCT n.track_id) AS DOUBLE) AS value
            FROM notes n
            JOIN __track_family f ON f.song_id=n.song_id AND f.track_id=n.track_id AND f.family='{fam}'
            WHERE n.song_id=?
            GROUP BY n.song_id, n.onset_bar
        """, [song_id])

def _repeat_score_from_chroma(con: duckdb.DuckDBPyConnection, song_id: str, window: int = 4) -> None:
    """
    Compute repeat_score_bar as max cosine similarity of each bar's chroma vector
    to any previous bar (within the same song).
    """
    # fetch per-bar chroma
    rows = con.execute("""
        SELECT bar,
               AVG(CASE WHEN feature='chroma_c_0_bar'  THEN value END) AS c0,
               AVG(CASE WHEN feature='chroma_c_1_bar'  THEN value END) AS c1,
               AVG(CASE WHEN feature='chroma_c_2_bar'  THEN value END) AS c2,
               AVG(CASE WHEN feature='chroma_c_3_bar'  THEN value END) AS c3,
               AVG(CASE WHEN feature='chroma_c_4_bar'  THEN value END) AS c4,
               AVG(CASE WHEN feature='chroma_c_5_bar'  THEN value END) AS c5,
               AVG(CASE WHEN feature='chroma_c_6_bar'  THEN value END) AS c6,
               AVG(CASE WHEN feature='chroma_c_7_bar'  THEN value END) AS c7,
               AVG(CASE WHEN feature='chroma_c_8_bar'  THEN value END) AS c8,
               AVG(CASE WHEN feature='chroma_c_9_bar'  THEN value END) AS c9,
               AVG(CASE WHEN feature='chroma_c_10_bar' THEN value END) AS c10,
               AVG(CASE WHEN feature='chroma_c_11_bar' THEN value END) AS c11
        FROM ts_bar
        WHERE song_id=? AND feature LIKE 'chroma_c_%_bar'
        GROUP BY bar
        ORDER BY bar
    """, [song_id]).fetchall()

    if not rows: 
        return

    bars = [r[0] for r in rows]
    V = np.array([r[1:] for r in rows], dtype=float)
    # normalize
    norms = np.linalg.norm(V, axis=1, keepdims=True) + 1e-9
    Vn = V / norms

    scores = np.zeros(len(bars))
    for i in range(len(bars)):
        if i == 0:
            scores[i] = 0.0
            continue
        # cosine against all previous bars (you can restrict to a window if needed)
        sims = Vn[:i] @ Vn[i].T
        scores[i] = float(np.max(sims))

    con.register("tmp_repeat", [(song_id, int(b), "repeat_score_bar", float(s)) for b, s in zip(bars, scores)])
    con.execute("INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) SELECT * FROM tmp_repeat")
    con.unregister("tmp_repeat")

    # Recurrence density per bar using affinity recurrence matrix
    try:
        X = Vn.T  # shape (features, time)
        R = librosa.segment.recurrence_matrix(X, mode='affinity', metric='cosine', sym=True)
        # density excluding self; normalize by (n-1)
        n = R.shape[0]
        denom = max(1, n - 1)
        dens = (np.sum(R, axis=1) - 1.0) / denom
        con.register("tmp_recd", [(song_id, int(b), "recurrence_density_bar", float(d)) for b, d in zip(bars, dens)])
        con.execute("INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) SELECT * FROM tmp_recd")
        con.unregister("tmp_recd")
        _compute_z_and_delta(con, song_id, "recurrence_density_bar")
    except Exception:
        pass

# ---------- Events ----------

def _emit_events(con: duckdb.DuckDBPyConnection, song_id: str, cfg: AudioConfig) -> None:
    # SECTION_BOUNDARY from novelty peaks
    con.execute("""
        WITH nov AS (
          SELECT song_id, bar, value,
                 value > LAG(value) OVER (PARTITION BY song_id ORDER BY bar) AS up,
                 value >= LEAD(value) OVER (PARTITION BY song_id ORDER BY bar) AS down,
                 (value - AVG(value) OVER (PARTITION BY song_id)) 
                   / NULLIF(STDDEV_SAMP(value) OVER (PARTITION BY song_id),0) AS z
          FROM ts_bar WHERE song_id=? AND feature='novelty_bar'
        )
        INSERT OR REPLACE INTO events (song_id, bar, event_type, detail, strength)
        SELECT song_id, bar, 'SECTION_BOUNDARY', NULL, z
        FROM nov WHERE up AND down AND z >= ?
    """, [song_id, cfg.boundary_z])

    # CLIMAX: max of energy or brightness z score
    con.execute("""
        WITH e AS (
          SELECT song_id, bar, value, 
                 RANK() OVER (PARTITION BY song_id ORDER BY value DESC) AS rnk
          FROM ts_bar WHERE song_id=? AND feature IN ('energy_bar_z','brightness_bar_z')
        )
        INSERT OR REPLACE INTO events (song_id, bar, event_type, detail, strength)
        SELECT song_id, bar, 'CLIMAX', NULL, value
        FROM e WHERE rnk=1
    """, [song_id])

    # DROP: large negative delta in energy
    con.execute("""
        INSERT OR REPLACE INTO events (song_id, bar, event_type, detail, strength)
        SELECT song_id, bar, 'DROP', NULL, value
        FROM ts_bar
        WHERE song_id=? AND feature='energy_bar_delta' AND value <= ?
    """, [song_id, cfg.drop_delta_thresh])

    # ENTRY_/EXIT_ from family counts (compare to previous bar)
    for fam, feat in [("DRUMS","active_drums"),("BASS","active_bass"),
                      ("PAD","active_pad"),("MELODY","active_melody")]:
        # entry
        con.execute(f"""
            WITH x AS (
              SELECT song_id, bar, value,
                     LAG(value) OVER (PARTITION BY song_id ORDER BY bar) AS prev
              FROM ts_bar WHERE song_id=? AND feature='{feat}'
            )
            INSERT OR REPLACE INTO events
            SELECT song_id, bar, 'ENTRY_{fam}', NULL, value
            FROM x WHERE COALESCE(prev,0)=0 AND value>0
        """, [song_id])
        # exit
        con.execute(f"""
            WITH x AS (
              SELECT song_id, bar, value,
                     LEAD(value) OVER (PARTITION BY song_id ORDER BY bar) AS nxt
              FROM ts_bar WHERE song_id=? AND feature='{feat}'
            )
            INSERT OR REPLACE INTO events
            SELECT song_id, bar, 'EXIT_{fam}', NULL, value
            FROM x WHERE value>0 AND COALESCE(nxt,0)=0
        """, [song_id])

    # CADENCE: simple V→I detection if chords present (count per bar)
    con.execute("""
        CREATE TEMP VIEW IF NOT EXISTS __chords AS
        SELECT onset_bar AS bar, rn
        FROM chords WHERE song_id=? ORDER BY onset_bar, onset_beat
    """, [song_id])
    # count V->I transitions occurring at this bar or previous
    con.execute("""
        WITH seq AS (
          SELECT bar, rn,
                 LAG(rn) OVER (ORDER BY bar) AS prev_rn
          FROM __chords
        ),
        vtoi AS (
          SELECT bar, 
                 CASE 
                   WHEN (prev_rn LIKE 'V%%' AND rn LIKE 'I%%') OR (prev_rn LIKE 'V/V%%' AND rn LIKE 'I%%') THEN 1
                   ELSE 0
                 END AS hit
          FROM seq
        ),
        agg AS (
          SELECT bar, SUM(hit) OVER (ORDER BY bar ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cadence_strength
          FROM vtoi
        )
        INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value)
        SELECT ?, bar, 'cadence_strength', CAST(cadence_strength AS DOUBLE) FROM agg
    """, [song_id])
    # emit CADENCE events where cadence_strength peaks
    con.execute("""
        WITH x AS (
          SELECT song_id, bar, value,
                 value >= COALESCE(LAG(value) OVER (PARTITION BY song_id ORDER BY bar),-1e9) AND
                 value >= COALESCE(LEAD(value) OVER (PARTITION BY song_id ORDER BY bar),-1e9) AS is_peak
          FROM ts_bar WHERE song_id=? AND feature='cadence_strength'
        )
        INSERT OR REPLACE INTO events
        SELECT song_id, bar, 'CADENCE', NULL, value FROM x WHERE is_peak AND value >= 1
    """, [song_id])

    # REPEAT_CHORUS: high repeat score in chorus sections (or fallback on high repeat anywhere)
    con.execute("""
        INSERT OR REPLACE INTO events (song_id, bar, event_type, detail, strength)
        SELECT tb.song_id, tb.bar, 'REPEAT_CHORUS', NULL, tb.value
        FROM ts_bar tb
        JOIN sections s ON s.song_id=tb.song_id
        WHERE tb.song_id=? AND tb.feature='repeat_score_bar'
          AND (LOWER(s.type)='chorus') AND tb.bar BETWEEN s.start_bar AND s.end_bar
          AND tb.value >= ?
    """, [song_id, 0.85])  # threshold tuned via cfg if you prefer

    # fallback: global repeats
    con.execute("""
        INSERT OR REPLACE INTO events (song_id, bar, event_type, detail, strength)
        SELECT song_id, bar, 'REPEAT_CHORUS', NULL, value
        FROM ts_bar
        WHERE song_id=? AND feature='repeat_score_bar' AND value >= ?
          AND NOT EXISTS (
            SELECT 1 FROM sections s WHERE s.song_id=? AND LOWER(s.type)='chorus'
          )
    """, [song_id, 0.92, song_id])

# ---------- Tags (stub) ----------

def _predict_tags(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    """
    Placeholder: attach simple tags based on features. Replace with Essentia/CLAP later.
    """
    # Mood heuristic from energy_z
    con.execute("""
        WITH avgz AS (
          SELECT AVG(value) AS z FROM ts_bar
          WHERE song_id=? AND feature='energy_bar_z'
        )
        INSERT OR REPLACE INTO tags_section (song_id, section_id, tag_type, tag, confidence)
        SELECT ?, 'S_global', 'mood',
               CASE WHEN z >= 0.5 THEN 'energetic'
                    WHEN z <= -0.5 THEN 'mellow'
                    ELSE 'neutral' END,
               ABS(z)
        FROM avgz
    """, [song_id, song_id])

# ---------- Public entry ----------

def run(song_id: str, midi_path: str, con: duckdb.DuckDBPyConnection, wav_out: Optional[str] = None, cfg: AudioConfig = AudioConfig()):
    # 1) Ensure symbolic family counts exist (ENTRY_/EXIT_ events depend on them)
    _ensure_symbolic_family_counts(con, song_id)

    # 2) Render (optional) and compute audio features
    if wav_out is None:
        os.makedirs("cache", exist_ok=True)
        wav_out = os.path.join("cache", f"{song_id}.wav")
    render_midi_to_wav(midi_path, wav_out, cfg)
    _audio_features_to_db(con, song_id, wav_out, cfg)

    # 3) Repeat score from chroma
    _repeat_score_from_chroma(con, song_id)

    # 4) Events
    _emit_events(con, song_id, cfg)

    # 5) Tags (stub)
    _predict_tags(con, song_id)
