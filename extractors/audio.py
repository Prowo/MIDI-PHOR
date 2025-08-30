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
import json

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
    # Essentia model paths (optional)
    emb_model_path: Optional[str] = None
    mood_model_path: Optional[str] = None
    mood_classes_json: Optional[str] = None
    genre_model_path: Optional[str] = None
    genre_classes_json: Optional[str] = None
    mood_top_n: int = 5
    mood_threshold: float = 0.02
    genre_top_n: int = 4
    genre_threshold: float = 0.05

# ---------- Rendering ----------

def render_midi_to_wav(midi_path: str, wav_out: str, cfg: AudioConfig) -> str:
    pm = pretty_midi.PrettyMIDI(midi_path)
    if cfg.soundfont_path:
        audio = pm.fluidsynth(fs=cfg.sr, sf2_path=cfg.soundfont_path)
    else:
        # fallback synth (simple): PrettyMIDI's built-in sine-wave synthesizer
        audio = pm.synthesize(fs=cfg.sr)
    # normalize + gain
    if np.max(np.abs(audio)) > 0:
        audio = audio / np.max(np.abs(audio))
    if cfg.render_gain_db != 0.0:
        audio = audio * (10 ** (cfg.render_gain_db / 20.0))
    sf.write(wav_out, audio, cfg.sr)
    return wav_out
def _ensure_bars_from_audio_if_missing(con: duckdb.DuckDBPyConnection, song_id: str, wav_path: str, cfg: AudioConfig) -> None:
    """
    If no symbolic bars exist, derive approximate bars from beat tracking (assume 4/4).
    Writes to bars(song_id, bar, start_sec, end_sec, num, den, qpm).
    """
    has_bars = con.execute("SELECT 1 FROM bars WHERE song_id=? LIMIT 1", [song_id]).fetchone()
    if has_bars:
        return
    try:
        y, sr = librosa.load(wav_path, sr=cfg.sr, mono=True)
        # Onset envelope for robust beat tracking
        onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=cfg.hop_length)
        tempo, beats = librosa.beat.beat_track(onset_envelope=onset_env, sr=sr, hop_length=cfg.hop_length)
        beat_times = librosa.frames_to_time(beats, sr=sr, hop_length=cfg.hop_length)
        if len(beat_times) == 0:
            return
        # Group every 4 beats into a bar (approx 4/4)
        bars = []
        bar_no = 1
        for i in range(0, len(beat_times), 4):
            start_sec = float(beat_times[i])
            if i + 4 < len(beat_times):
                end_sec = float(beat_times[i + 4])
            else:
                # last partial bar: extend to last audio time or last beat
                end_sec = float(beat_times[-1])
            bars.append((song_id, bar_no, start_sec, end_sec, 4, 4, float(tempo)))
            bar_no += 1
        if bars:
            con.executemany(
                "INSERT OR REPLACE INTO bars (song_id, bar, start_sec, end_sec, num, den, qpm) VALUES (?, ?, ?, ?, ?, ?, ?)",
                bars,
            )
    except Exception:
        # non-fatal: continue without bar aggregation
        pass


# ---------- Frames → DB ----------

def _store_frames(con: duckdb.DuckDBPyConnection, song_id: str, feature: str, t_ms: np.ndarray, values: np.ndarray) -> None:
    if len(values) == 0:
        return
    rows = [(song_id, feature, int(ms), float(ms) / 1000.0, float(v)) for ms, v in zip(t_ms.astype(int), values)]
    con.executemany(
        "INSERT OR REPLACE INTO ts_frame (song_id, feature, t_ms, t_sec, value) VALUES (?, ?, ?, ?, ?)",
        rows,
    )

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
    # Avoid prepared parameter in DDL by formatting song_id directly (safe: song_id is internal id)
    con.execute(f"""
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
                  WHEN t.gm_program BETWEEN 32 AND 39 THEN 'bass'
                  WHEN t.gm_program BETWEEN 88 AND 95 THEN 'pad'
                  WHEN t.gm_program BETWEEN 80 AND 87 THEN 'melody'
                  WHEN t.gm_program BETWEEN 115 AND 119 THEN 'drums'
                  ELSE 'other'
                END
          END AS family
        FROM tracks t
        WHERE t.song_id='{song_id}'
    """)

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

    rep_rows = [(song_id, int(b), "repeat_score_bar", float(s)) for b, s in zip(bars, scores)]
    con.executemany(
        "INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) VALUES (?, ?, ?, ?)",
        rep_rows,
    )

    # Recurrence density per bar using affinity recurrence matrix
    try:
        X = Vn.T  # shape (features, time)
        R = librosa.segment.recurrence_matrix(X, mode='affinity', metric='cosine', sym=True)
        # density excluding self; normalize by (n-1)
        n = R.shape[0]
        denom = max(1, n - 1)
        dens = (np.sum(R, axis=1) - 1.0) / denom
        rec_rows = [(song_id, int(b), "recurrence_density_bar", float(d)) for b, d in zip(bars, dens)]
        con.executemany(
            "INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) VALUES (?, ?, ?, ?)",
            rec_rows,
        )
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
    con.execute(f"""
        CREATE TEMP VIEW IF NOT EXISTS __chords AS
        SELECT onset_bar AS bar, rn
        FROM chords WHERE song_id='{song_id}' ORDER BY onset_bar, onset_beat
    """)
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
    Attach audio tags using Essentia MTG models if configured; otherwise fallback to energy heuristic.
    """
    try:
        from essentia.standard import MonoLoader, TensorflowPredictEffnetDiscogs, TensorflowPredict2D
        cfg_rows = con.execute("SELECT ? AS emb, ? AS mood_m, ? AS mood_c, ? AS genre_m, ? AS genre_c", [
            None, None, None, None, None
        ]).fetchone()
        # Not used: AudioConfig is not accessible here directly; rely on defaults/fallback
        wav_path = None
        # Attempt to find cached render
        cand = os.path.join("cache", f"{song_id}.wav")
        if os.path.exists(cand):
            wav_path = cand
        # or check common alt path
        alt = os.path.join("clean_audio", f"{song_id}.wav")
        if wav_path is None and os.path.exists(alt):
            wav_path = alt
        if wav_path is None:
            raise RuntimeError("no audio file found for tagging")

        audio = MonoLoader(filename=wav_path, sampleRate=16000, resampleQuality=1)()
        if len(audio) < 16000 * 3:
            raise RuntimeError("audio too short for tagging")

        # These will raise if models are missing; fall through to heuristic
        emb_model = TensorflowPredictEffnetDiscogs(graphFilename=os.environ.get("ESS_EMB_MODEL",""), output="PartitionedCall:1")
        mood_model = TensorflowPredict2D(graphFilename=os.environ.get("ESS_MOOD_MODEL",""))
        genre_model = TensorflowPredict2D(graphFilename=os.environ.get("ESS_GENRE_MODEL",""))
        mood_json = os.environ.get("ESS_MOOD_JSON","")
        genre_json = os.environ.get("ESS_GENRE_JSON","")

        def _top_tags(emb, model, classes_json, top_n, thr):
            with open(classes_json, "r") as f:
                meta = json.load(f)
            preds = model(emb)
            mean_act = np.mean(preds, axis=0)
            idx = np.argpartition(mean_act, -top_n)[-top_n:]
            pairs = [(meta['classes'][i], float(mean_act[i])) for i in idx if mean_act[i] >= thr]
            pairs.sort(key=lambda x: -x[1])
            return pairs

        emb = emb_model(audio)
        mood_pairs = _top_tags(emb, mood_model, mood_json, top_n=5, thr=0.02)
        genre_pairs = _top_tags(emb, genre_model, genre_json, top_n=4, thr=0.05)

        # write to tags_section at global scope
        rows = []
        for tag, conf in mood_pairs:
            rows.append((song_id, 'S_global', 'mood', tag, float(conf)))
        for tag, conf in genre_pairs:
            rows.append((song_id, 'S_global', 'genre', tag, float(conf)))
        if rows:
            con.executemany(
                "INSERT OR REPLACE INTO tags_section (song_id, section_id, tag_type, tag, confidence) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        return
    except Exception:
        pass

    # Fallback: Mood heuristic from energy_z quartiles (zero-mean guard)
    con.execute("""
        WITH ez AS (
          SELECT value FROM ts_bar WHERE song_id=? AND feature='energy_bar_z'
        ),
        q AS (
          SELECT quantile_cont(value, 0.75) AS q75,
                 quantile_cont(value, 0.25) AS q25
          FROM ez
        )
        INSERT OR REPLACE INTO tags_section (song_id, section_id, tag_type, tag, confidence)
        SELECT ?, 'S_global', 'mood',
               CASE WHEN q75 >= 0.5 THEN 'energetic'
                    WHEN q25 <= -0.5 THEN 'mellow'
                    ELSE 'neutral' END,
               CASE WHEN q75 >= ABS(q25) THEN q75 ELSE ABS(q25) END
        FROM q
    """, [song_id, song_id])

    # Heuristic genre tags using spectral + rhythm summaries
    stats = con.execute("""
        SELECT
          AVG(CASE WHEN feature='brightness_bar_z' THEN value END) AS bright_z,
          AVG(CASE WHEN feature='rolloff_bar_z'    THEN value END) AS roll_z,
          AVG(CASE WHEN feature='flatness_bar_z'   THEN value END) AS flat_z,
          AVG(CASE WHEN feature='zcr_bar_z'        THEN value END) AS zcr_z,
          AVG(CASE WHEN feature='onset_strength_bar_z' THEN value END) AS onset_z,
          AVG(CASE WHEN feature='tempo_bar'        THEN value END) AS tempo_avg,
          AVG(CASE WHEN feature='repeat_score_bar' THEN value END) AS rep_avg,
          AVG(CASE WHEN feature='active_drums'     THEN value END) AS drums_avg
        FROM ts_bar WHERE song_id=?
    """, [song_id]).fetchone()
    if stats is not None:
        bright_z, roll_z, flat_z, zcr_z, onset_z, tempo_avg, rep_avg, drums_avg = [s if s is not None else 0.0 for s in stats]

        def clip01(x: float) -> float:
            return float(max(0.0, min(1.0, x)))

        # Simple scores
        electronic_score = clip01(max(flat_z, zcr_z) / 2.0 + max(0.0, roll_z) * 0.1)
        rock_score = clip01((max(0.0, bright_z) + max(0.0, onset_z)) / 2.0 + (drums_avg or 0.0) * 0.1)
        pop_score = clip01((rep_avg or 0.0) * 0.6 + (1.0 - electronic_score) * 0.2 + (1.0 if 90 <= (tempo_avg or 0.0) <= 130 else 0.0) * 0.2)
        acoustic_score = clip01((max(0.0, -flat_z) + max(0.0, -zcr_z)) / 2.0 + max(0.0, -bright_z) * 0.2)
        ambient_score = clip01((max(0.0, -onset_z) + max(0.0, -bright_z)) / 2.0 + (1.0 if (tempo_avg or 0.0) < 80 else 0.0) * 0.2)

        genre_pairs = [
            ("electronic", electronic_score),
            ("rock", rock_score),
            ("pop", pop_score),
            ("acoustic", acoustic_score),
            ("ambient", ambient_score),
        ]
        genre_pairs = [(g, float(round(c, 3))) for g, c in genre_pairs if c >= 0.25]
        genre_pairs.sort(key=lambda x: -x[1])
        genre_pairs = genre_pairs[:3]
        if genre_pairs:
            con.executemany(
                "INSERT OR REPLACE INTO tags_section (song_id, section_id, tag_type, tag, confidence) VALUES (?, 'S_global', 'genre', ?, ?)",
                [(song_id, g, c) for g, c in genre_pairs],
            )

# ---------- Public entry ----------

def run(song_id: str, midi_path: Optional[str], con: duckdb.DuckDBPyConnection, wav_out: Optional[str] = None, audio_in: Optional[str] = None, cfg: AudioConfig = AudioConfig()):
    # 1) Ensure symbolic family counts exist (ENTRY_/EXIT_ events depend on them)
    _ensure_symbolic_family_counts(con, song_id)

    # 2) Determine WAV source: provided audio or render from MIDI
    if audio_in is not None and os.path.exists(audio_in):
        wav_path = audio_in
    else:
        if wav_out is None:
            os.makedirs("cache", exist_ok=True)
            wav_out = os.path.join("cache", f"{song_id}.wav")
        if midi_path is None:
            raise ValueError("midi_path is required when audio_in is not provided")
        render_midi_to_wav(midi_path, wav_out, cfg)
        wav_path = wav_out

    # 3) Ensure bars exist (from symbolic; if missing, approximate from audio)
    _ensure_bars_from_audio_if_missing(con, song_id, wav_path, cfg)

    # 4) Compute audio features
    _audio_features_to_db(con, song_id, wav_path, cfg)

    # 5) Repeat score from chroma
    _repeat_score_from_chroma(con, song_id)

    # 6) Events (best-effort)
    try:
        _emit_events(con, song_id, cfg)
    except Exception:
        pass

    # 7) Tags (stub)
    _predict_tags(con, song_id)
