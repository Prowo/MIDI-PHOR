# extractors/symbolic.py
# pip install miditoolkit pretty_midi music21 pandas numpy

from __future__ import annotations
import json, math, uuid
from dataclasses import dataclass
from typing import List, Tuple
import numpy as np
import pandas as pd
import pretty_midi as pm
import miditoolkit
from music21 import converter, roman

from db.duck import upsert_df

@dataclass
class SymbolicConfig:
    onset_tol_sec: float = 0.05        # clustering tolerance for chord-ish simultaneity
    syncop_eps: float = 0.15           # beat distance to count as "on-beat"
    beat_subdiv: int = 4               # 16th-note resolution for polyphony sampling
    pedal_threshold: int = 64          # sustain CC threshold
    top_motif_n: int = 3               # top N motifs to store
    motif_n: int = 3                   # n-gram length for motifs

# ---------- helpers

def _gm_role(is_drum: bool, mean_pitch: float, is_monophonic: bool) -> str:
    if is_drum: return "perc"
    if mean_pitch <= 50: return "bass"
    if is_monophonic and mean_pitch >= 60: return "melody"
    return "comp"

def _ts_segments(mtk) -> List[Tuple[int,int,int]]:
    """Return [(tick_start, num, den), ...] sorted."""
    ts = sorted(mtk.time_signature_changes, key=lambda t: t.time)
    if not ts:
        # default 4/4 at tick 0
        return [(0, 4, 4)]
    return [(c.time, c.numerator, c.denominator) for c in ts]

def _tempo_at(bar_start_sec: float, pm_obj: pm.PrettyMIDI) -> float:
    times, tempi = pm_obj.get_tempo_changes()
    idx = np.searchsorted(times, bar_start_sec, side="right") - 1
    idx = max(0, min(idx, len(tempi)-1))
    return float(tempi[idx])

def _build_bars(mtk, pm_obj) -> pd.DataFrame:
    segs = _ts_segments(mtk)
    last_tick = max((n.end for inst in mtk.instruments for n in inst.notes), default=0)
    tpb = mtk.ticks_per_beat

    rows = []
    bar_no = 1
    for i, (seg_tick, num, den) in enumerate(segs):
        seg_end = segs[i+1][0] if i+1 < len(segs) else last_tick
        beats_per_bar = num * (4.0/den)
        # ensure integer ticks to avoid PrettyMIDI warnings
        ticks_per_bar = int(round(tpb * beats_per_bar))
        tick = seg_tick
        while tick < seg_end + 1:
            start_sec = pm_obj.tick_to_time(int(round(tick)))
            end_tick = min(tick + ticks_per_bar, seg_end)
            end_sec = pm_obj.tick_to_time(int(round(end_tick)))
            qpm = _tempo_at(start_sec, pm_obj)
            rows.append((bar_no, int(round(tick)), int(round(end_tick)), start_sec, end_sec, num, den, qpm))
            bar_no += 1
            tick += ticks_per_bar

    df = pd.DataFrame(rows, columns=["bar","start_tick","end_tick","start_sec","end_sec","num","den","qpm"])
    return df

def _tick_to_bar_beat(tick: int, mtk, bars_df: pd.DataFrame) -> Tuple[int,float]:
    """Map tick → (bar, beat_in_bar), using bars_df computed above."""
    # Fast: use per-segment math
    segs = _ts_segments(mtk)
    tpb = mtk.ticks_per_beat

    acc_bars = 1
    for i, (seg_tick, num, den) in enumerate(segs):
        seg_end = segs[i+1][0] if i+1 < len(segs) else 10**12
        beats_per_bar = num * (4.0/den)
        ticks_per_bar = tpb * beats_per_bar
        if tick < seg_end:
            bars_since = math.floor((tick - seg_tick) / ticks_per_bar + 1e-9)
            bar = acc_bars + bars_since
            within = (tick - seg_tick) - bars_since * ticks_per_bar
            beat = within / tpb + 1.0  # 1-based beat
            return int(bar), float(beat)
        # advance acc_bars to next segment
        seg_bars = math.floor((seg_end - seg_tick) / ticks_per_bar + 1e-9)
        acc_bars += seg_bars
    # fallback
    last_bar = int(bars_df["bar"].max())
    return last_bar, 1.0

def _polyphony_curve(notes_df: pd.DataFrame, bars_df: pd.DataFrame, beat_subdiv=4) -> pd.Series:
    """
    Average number of simultaneously active notes per bar (tempo-invariant).

    This uses an event-sweep integration in tick time (exact time-average within each bar),
    avoiding the previous O(#bars * #grid * #notes) sampling loop.
    """
    if notes_df.empty or bars_df.empty:
        return pd.Series(np.zeros(len(bars_df), dtype=float), index=bars_df.bar.values, dtype=float)

    if not {"onset_tick", "offset_tick"}.issubset(notes_df.columns) or not {"start_tick", "end_tick"}.issubset(bars_df.columns):
        # Fallback: coarse seconds-based sampling.
        out = []
        for _, b in bars_df.iterrows():
            beats = float(b["num"]) * (4.0 / float(b["den"]))
            steps = max(1, int(round(beats * beat_subdiv)))
            ts = np.linspace(float(b.start_sec), float(b.end_sec), steps, endpoint=False)
            active = []
            for t in ts:
                m = (notes_df.onset_sec <= t) & (notes_df.offset_sec > t)
                active.append(int(m.sum()))
            out.append(float(np.mean(active)) if active else 0.0)
        return pd.Series(out, index=bars_df.bar.values, dtype=float)

    on = notes_df["onset_tick"].astype(np.int64).to_numpy(copy=False)
    off = notes_df["offset_tick"].astype(np.int64).to_numpy(copy=False)
    valid = off > on
    on = on[valid]
    off = off[valid]
    if on.size == 0:
        return pd.Series(np.zeros(len(bars_df), dtype=float), index=bars_df.bar.values, dtype=float)

    ticks = np.concatenate([on, off])
    delta = np.concatenate([np.ones_like(on, dtype=np.int32), -np.ones_like(off, dtype=np.int32)])
    order = np.lexsort((delta, ticks))  # note-offs (-1) before note-ons (+1) at same tick
    ticks = ticks[order]
    delta = delta[order]

    starts = bars_df["start_tick"].astype(np.int64).to_numpy(copy=False)
    ends = bars_df["end_tick"].astype(np.int64).to_numpy(copy=False)
    bars = bars_df["bar"].astype(int).to_numpy(copy=False)

    out = np.zeros(len(bars), dtype=float)
    ei = 0
    cur = 0

    for bi in range(len(bars)):
        b0 = int(starts[bi])
        b1 = int(ends[bi])
        if b1 <= b0:
            out[bi] = 0.0
            continue

        while ei < len(ticks) and int(ticks[ei]) < b0:
            cur += int(delta[ei])
            ei += 1

        acc = 0.0
        t = b0
        while ei < len(ticks) and int(ticks[ei]) < b1:
            te = int(ticks[ei])
            if te > t:
                acc += cur * (te - t)
                t = te
            cur += int(delta[ei])
            ei += 1
        if b1 > t:
            acc += cur * (b1 - t)

        out[bi] = float(acc) / float(b1 - b0)

    return pd.Series(out, index=bars, dtype=float)

def _backbeat_strength(notes_df: pd.DataFrame, bars_df: pd.DataFrame, eps=0.2) -> pd.Series:
    # Only for 4/4 bars; sum drum velocities near beats 2 and 4 / total drum velocity
    if notes_df.empty or bars_df.empty:
        return pd.Series(np.zeros(len(bars_df), dtype=float), index=bars_df.bar.values, dtype=float)

    drum = notes_df.loc[notes_df["role"] == "perc", ["onset_bar", "onset_beat", "velocity"]].copy()
    if drum.empty:
        bb = pd.Series(np.zeros(len(bars_df), dtype=float), index=bars_df.bar.values, dtype=float)
        is_44 = (bars_df["num"].astype(int) == 4) & (bars_df["den"].astype(int) == 4)
        bb.loc[~is_44.values] = np.nan
        return bb

    beat = drum["onset_beat"].astype(float)
    is_bb = (np.abs(beat - 2.0) <= eps) | (np.abs(beat - 4.0) <= eps)
    drum["bb_vel"] = drum["velocity"].astype(float) * is_bb.astype(float)
    drum["tot_vel"] = drum["velocity"].astype(float)
    agg = drum.groupby("onset_bar")[["bb_vel", "tot_vel"]].sum()

    bb = (agg["bb_vel"] / (agg["tot_vel"] + 1e-6)).reindex(bars_df.bar.values, fill_value=0.0).astype(float)
    is_44 = (bars_df["num"].astype(int) == 4) & (bars_df["den"].astype(int) == 4)
    bb.loc[~is_44.values] = np.nan
    bb.index = bars_df.bar.values
    return bb

def _syncopation_index(notes_df: pd.DataFrame, bars_df: pd.DataFrame, eps=0.15) -> pd.Series:
    # Ratio of non-integer-beat onsets (weighted by velocity)
    if notes_df.empty or bars_df.empty:
        return pd.Series(np.zeros(len(bars_df), dtype=float), index=bars_df.bar.values, dtype=float)

    non_drum = notes_df.loc[notes_df["role"] != "perc", ["onset_bar", "onset_beat", "velocity"]].copy()
    if non_drum.empty:
        return pd.Series(np.zeros(len(bars_df), dtype=float), index=bars_df.bar.values, dtype=float)

    beat = non_drum["onset_beat"].astype(float)
    frac = np.abs(beat - np.round(beat))
    is_off = (frac > eps).astype(float)
    non_drum["off_vel"] = non_drum["velocity"].astype(float) * is_off
    non_drum["tot_vel"] = non_drum["velocity"].astype(float)
    agg = non_drum.groupby("onset_bar")[["off_vel", "tot_vel"]].sum()

    out = (agg["off_vel"] / (agg["tot_vel"] + 1e-6)).reindex(bars_df.bar.values, fill_value=0.0)
    out.index = bars_df.bar.values
    return out.astype(float)


def _map_ticks_to_bar_beat(onset_ticks: np.ndarray, bars_df: pd.DataFrame, tpb: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Vectorized mapping from onset_tick -> (bar, beat_in_bar) using bars_df computed above.
    """
    starts = bars_df["start_tick"].astype(np.int64).to_numpy(copy=False)
    bars = bars_df["bar"].astype(np.int64).to_numpy(copy=False)
    nums = bars_df["num"].astype(np.int64).to_numpy(copy=False)
    dens = bars_df["den"].astype(np.int64).to_numpy(copy=False)

    idx = np.searchsorted(starts, onset_ticks.astype(np.int64), side="right") - 1
    idx = np.clip(idx, 0, len(starts) - 1)

    bar = bars[idx]
    beat = (onset_ticks.astype(np.float64) - starts[idx].astype(np.float64)) / float(tpb) + 1.0
    beats_per_bar = nums[idx].astype(np.float64) * (4.0 / dens[idx].astype(np.float64))
    beat = np.minimum(np.maximum(beat, 1.0), beats_per_bar + 1e-3)
    return bar.astype(int), beat.astype(float)

def _infer_chords_romans(m21_stream, k_est=None) -> pd.DataFrame:
    # Caller should reuse a pre-parsed music21 stream (converter.parse is expensive).
    k = k_est if k_est is not None else m21_stream.analyze("Krumhansl")
    seq = []
    ch = m21_stream.chordify()
    for c in ch.recurse().getElementsByClass("Chord"):
        if len({p.pitchClass for p in c.pitches}) < 2:  # skip singletons
            continue
        # IMPORTANT: keep `c` bound to the stream so measure/beat are correct.
        # Use a closed-position copy only for chord spelling / RN inference.
        c_closed = c.closedPosition(forceOctave=4)
        rn = roman.romanNumeralFromChord(c_closed, k).figure
        # measureNumber and beat are available on chord
        bar = getattr(c, "measureNumber", None)
        beat = float(getattr(c, "beat", 1.0))
        dur_beats = float(getattr(c, "quarterLength", 1.0))
        name = c_closed.pitchedCommonName  # e.g., 'C major triad'
        root_pc = int(c_closed.root().pitchClass)
        quality = rn[0] if rn else "other"
        chord_id = str(uuid.uuid4())[:12]
        seq.append((
            chord_id,
            bar,
            beat,
            dur_beats,
            name,
            rn,
            root_pc,
            quality,
            "{"+",".join(map(str, sorted({p.pitchClass for p in c_closed.pitches})))+"}",
        ))
    df = pd.DataFrame(seq, columns=["chord_id","onset_bar","onset_beat","dur_beats","name","rn","root_pc","quality","pcset"])
    if not df.empty:
        df = df.drop_duplicates(subset=["onset_bar", "onset_beat", "rn", "name"], keep="first").reset_index(drop=True)
    return df

def _top_motifs(notes_df: pd.DataFrame, cfg: SymbolicConfig) -> pd.DataFrame:
    # pick a melody-like track (monophonic, highest register)
    tracks = notes_df.groupby("track_id").agg(
        n=("note_id","count"),
        mono=("pitch", lambda x: (x.value_counts().max() == 1)),
        mean_pitch=("pitch","mean")
    ).reset_index()
    if tracks.empty:
        return pd.DataFrame(columns=["motif_id","pattern","occurrences","support"])
    cand = tracks.sort_values(["mono","mean_pitch","n"], ascending=[False,False,False]).head(1)
    melod_tid = cand["track_id"].values[0]

    mel = notes_df.query("track_id == @melod_tid").sort_values("onset_sec")
    if len(mel) < cfg.motif_n + 1:
        return pd.DataFrame(columns=["motif_id","pattern","occurrences","support"])

    # intervals (semitones) and durations (quantized to 1/8)
    ivals = mel["pitch"].diff().dropna().astype(int).tolist()
    durs_q = (mel["dur_beats"].clip(lower=1/8).round(3) * 8).round().astype(int).astype(str).tolist()  # 1/8 units (rough)
    # build n-grams on combined token stream length len-1 for ivals, len for durs; align by starts
    tokens = [f"i{ivals[i]}|d{durs_q[i]}" for i in range(min(len(ivals), len(durs_q)))]
    from collections import Counter, defaultdict
    counts = Counter(tuple(tokens[i:i+cfg.motif_n]) for i in range(len(tokens)-cfg.motif_n+1))
    occs = defaultdict(list)
    for i in range(len(tokens)-cfg.motif_n+1):
        key = tuple(tokens[i:i+cfg.motif_n])
        bar = int(mel.iloc[i]["onset_bar"])
        occs[key].append({"bar": bar})

    rows=[]
    for key, cnt in counts.most_common(cfg.top_motif_n):
        pat = " ".join(key).replace("|",":")
        rows.append((str(uuid.uuid4())[:12], pat, json.dumps(occs[key]), int(cnt)))
    return pd.DataFrame(rows, columns=["motif_id","pattern","occurrences","support"])

# ---------- main entry

def _mirror_bar_metrics_to_ts_bar(con, bar_metrics_df: pd.DataFrame) -> None:
    if bar_metrics_df.empty:
        return
    # Melt to (song_id, bar, feature, value)
    long_df = bar_metrics_df.melt(id_vars=["song_id","bar"], var_name="feature", value_name="value")
    # ensure DOUBLE cast
    tmp = long_df[["song_id","bar","feature","value"]].copy()
    from math import isnan
    # NaNs are allowed; DuckDB will store NULL
    con.register("tmp_tsbar", tmp)
    con.execute("""
        INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value)
        SELECT song_id, CAST(bar AS INTEGER), feature, CAST(value AS DOUBLE)
        FROM tmp_tsbar
    """)
    con.unregister("tmp_tsbar")

def _symbolic_chroma_matrix(notes_df: pd.DataFrame, bars_df: pd.DataFrame) -> np.ndarray:
    """
    Build a per-bar 12D pitch-class histogram from symbolic notes.
    Uses onset_bar and weights by dur_beats * (velocity/127).
    Returns shape (n_bars, 12) aligned to bars_df.bar order, L1-normalized per bar.
    """
    bars = bars_df["bar"].astype(int).to_list()
    if not bars:
        return np.zeros((0, 12), dtype=float)

    if notes_df.empty:
        return np.zeros((len(bars), 12), dtype=float)

    x = notes_df.copy()
    x["bar"] = x["onset_bar"].astype(int)
    x["pc"] = (x["pitch"].astype(int) % 12).astype(int)
    # Weight by duration only (keeps harmonic/structure features invariant to velocity scaling).
    x["w"] = x["dur_beats"].astype(float).clip(lower=1e-6)

    g = x.groupby(["bar", "pc"])["w"].sum()

    bar_to_idx = {b: i for i, b in enumerate(bars)}
    V = np.zeros((len(bars), 12), dtype=float)
    for (bar, pc), w in g.items():
        i = bar_to_idx.get(int(bar))
        if i is None:
            continue
        V[i, int(pc)] = float(w)

    s = V.sum(axis=1, keepdims=True)
    V = np.divide(V, s, out=np.zeros_like(V), where=(s > 0))
    return V

def _write_symbolic_structure_ts_bar(con, song_id: str, notes_df: pd.DataFrame, bars_df: pd.DataFrame) -> None:
    """
    Populate ts_bar with symbolic substitutes for audio-derived structure features:
      - chroma_c_{0..11}_bar
      - novelty_bar (bar-to-bar chroma distance)
      - repeat_score_bar (max cosine similarity to any previous bar)
      - recurrence_density_bar (mean cosine similarity to all other bars)
    """
    V = _symbolic_chroma_matrix(notes_df, bars_df)
    if V.shape[0] == 0:
        return

    bars = bars_df["bar"].astype(int).to_list()
    rows = []

    for bi, bar in enumerate(bars):
        for k in range(12):
            rows.append((song_id, int(bar), f"chroma_c_{k}_bar", float(V[bi, k])))

    nov = np.zeros(len(bars), dtype=float)
    if len(bars) >= 2:
        nov[1:] = np.linalg.norm(V[1:] - V[:-1], axis=1)
    for bi, bar in enumerate(bars):
        rows.append((song_id, int(bar), "novelty_bar", float(nov[bi])))

    norms = np.linalg.norm(V, axis=1, keepdims=True) + 1e-9
    Vn = V / norms
    S = Vn @ Vn.T

    rep = np.zeros(len(bars), dtype=float)
    for i in range(len(bars)):
        rep[i] = float(np.max(S[i, :i])) if i > 0 else 0.0
    denom = max(1, len(bars) - 1)
    dens = (np.sum(S, axis=1) - np.diag(S)) / float(denom)

    for bi, bar in enumerate(bars):
        rows.append((song_id, int(bar), "repeat_score_bar", float(rep[bi])))
        rows.append((song_id, int(bar), "recurrence_density_bar", float(dens[bi])))

    con.executemany(
        "INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) VALUES (?, ?, ?, ?)",
        rows,
    )

def _write_symbolic_tags_section(con, song_id: str, tracks_df: pd.DataFrame, bars_df: pd.DataFrame, bar_metrics_df: pd.DataFrame) -> None:
    """
    Populate tags_section with MIDI-derived tags (no rendered audio assumptions).
    Writes global tags at section_id='S_global'.
    """
    tags: List[tuple] = []

    # Meter
    try:
        num = int(bars_df.iloc[0]["num"])
        den = int(bars_df.iloc[0]["den"])
        tags.append((song_id, "S_global", "symbolic", f"{num}/{den}", 0.9))
    except Exception:
        pass

    # Mode (from key_changes if present)
    try:
        row = con.execute(
            "SELECT key FROM key_changes WHERE song_id=? ORDER BY at_bar, at_beat LIMIT 1",
            [song_id],
        ).fetchone()
        if row and row[0]:
            key_str = str(row[0])
            if ":" in key_str:
                _, mode = key_str.split(":", 1)
                mode = mode.strip().lower()
                if mode.startswith("maj"):
                    tags.append((song_id, "S_global", "symbolic", "major", 0.75))
                elif mode.startswith("min"):
                    tags.append((song_id, "S_global", "symbolic", "minor", 0.75))
    except Exception:
        pass

    # Tempo class
    try:
        bpm = float(bars_df["qpm"].astype(float).mean())
        if bpm < 80:
            tags.append((song_id, "S_global", "symbolic", "slow", 0.7))
        elif bpm <= 140:
            tags.append((song_id, "S_global", "symbolic", "midtempo", 0.6))
        else:
            tags.append((song_id, "S_global", "symbolic", "fast", 0.7))
    except Exception:
        pass

    # Ensemble/roles
    try:
        n_tracks = int(len(tracks_df))
        if n_tracks <= 4:
            tags.append((song_id, "S_global", "symbolic", "small_ensemble", 0.6))
        elif n_tracks <= 8:
            tags.append((song_id, "S_global", "symbolic", "medium_ensemble", 0.6))
        else:
            tags.append((song_id, "S_global", "symbolic", "large_ensemble", 0.6))

        roles = (tracks_df["role"].astype(str).str.lower().value_counts().to_dict() if "role" in tracks_df.columns else {})
        if roles.get("perc", 0) > 0:
            tags.append((song_id, "S_global", "symbolic", "has_drums", 0.7))
        if roles.get("bass", 0) > 0:
            tags.append((song_id, "S_global", "symbolic", "has_bass", 0.7))
        if roles.get("melody", 0) > 0:
            tags.append((song_id, "S_global", "symbolic", "has_melody", 0.6))
    except Exception:
        pass

    # Texture/rhythm traits from bar_metrics
    try:
        if not bar_metrics_df.empty:
            d = float(np.nanmean(bar_metrics_df["density"].values))
            p = float(np.nanmean(bar_metrics_df["polyphony"].values))
            bb_vals = bar_metrics_df["backbeat_strength"].values
            bb = float(np.nanmean(bb_vals)) if not np.all(np.isnan(bb_vals)) else float("nan")
            sy = float(np.nanmean(bar_metrics_df["syncopation"].values))

            if d < 0.6:
                tags.append((song_id, "S_global", "symbolic", "sparse", 0.6))
            elif d > 1.4:
                tags.append((song_id, "S_global", "symbolic", "dense", 0.6))

            if p < 1.5:
                tags.append((song_id, "S_global", "symbolic", "mostly_monophonic", 0.55))
            elif p > 3.0:
                tags.append((song_id, "S_global", "symbolic", "polyphonic", 0.55))

            if not np.isnan(bb) and bb > 0.55:
                tags.append((song_id, "S_global", "symbolic", "strong_backbeat", 0.55))
            if not np.isnan(sy) and sy > 0.35:
                tags.append((song_id, "S_global", "symbolic", "syncopated", 0.55))
    except Exception:
        pass

    if not tags:
        return
    con.executemany(
        "INSERT OR REPLACE INTO tags_section (song_id, section_id, tag_type, tag, confidence) VALUES (?, ?, ?, ?, ?)",
        tags,
    )

def _write_symbolic_role_activity_ts_bar(con, song_id: str, notes_df: pd.DataFrame, tracks_df: pd.DataFrame, bars_df: pd.DataFrame) -> None:
    """
    Write bar-level activity counts into ts_bar using symbolic notes+roles:
      active_tracks, active_drums, active_bass, active_pad, active_melody
    """
    if notes_df.empty or bars_df.empty:
        return
    # notes_df has role column at this point
    bars = bars_df["bar"].astype(int).to_list()
    roles = {"perc": "active_drums", "bass": "active_bass", "comp": "active_pad", "melody": "active_melody"}

    # total active tracks per bar
    act = notes_df.groupby("onset_bar")["track_id"].nunique().reindex(bars, fill_value=0)
    rows = [(song_id, int(b), "active_tracks", float(act.loc[b])) for b in bars]

    for role, feat in roles.items():
        rr = notes_df.loc[notes_df["role"] == role].groupby("onset_bar")["track_id"].nunique().reindex(bars, fill_value=0)
        rows.extend([(song_id, int(b), feat, float(rr.loc[b])) for b in bars])

    con.executemany(
        "INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) VALUES (?, ?, ?, ?)",
        rows,
    )

def _write_symbolic_pitch_texture_ts_bar(con, song_id: str, notes_df: pd.DataFrame, bars_df: pd.DataFrame) -> None:
    """
    Write bar-level pitch texture metrics into ts_bar:
      pitch_range, pitch_mean, pitch_std
    """
    if notes_df.empty or bars_df.empty:
        return
    bars = bars_df["bar"].astype(int).to_list()
    g = notes_df.groupby("onset_bar")["pitch"]
    pmin = g.min().reindex(bars)
    pmax = g.max().reindex(bars)
    pmean = g.mean().reindex(bars)
    pstd = g.std().reindex(bars)
    rows = []
    for b in bars:
        if pd.notna(pmin.loc[b]) and pd.notna(pmax.loc[b]):
            rows.append((song_id, int(b), "pitch_range", float(pmax.loc[b] - pmin.loc[b])))
        if pd.notna(pmean.loc[b]):
            rows.append((song_id, int(b), "pitch_mean", float(pmean.loc[b])))
        if pd.notna(pstd.loc[b]):
            rows.append((song_id, int(b), "pitch_std", float(pstd.loc[b])))
    if rows:
        con.executemany(
            "INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) VALUES (?, ?, ?, ?)",
            rows,
        )

def _write_symbolic_cadence_ts_and_events(con, song_id: str) -> None:
    """
    Symbolic-only cadence detector based on Roman numeral transitions.
    Writes:
      - ts_bar.cadence_strength (cumulative V->I hits)
      - events.CADENCE peaks
    """
    try:
        con.execute(f"""
            CREATE TEMP VIEW IF NOT EXISTS __chords_sym AS
            SELECT onset_bar AS bar, rn
            FROM chords WHERE song_id='{song_id}' ORDER BY onset_bar, onset_beat
        """)
        con.execute("""
            WITH seq AS (
              SELECT bar, rn,
                     LAG(rn) OVER (ORDER BY bar) AS prev_rn
              FROM __chords_sym
            ),
            vtoi AS (
              SELECT bar,
                     CASE
                       WHEN (UPPER(prev_rn) LIKE 'V%%' AND UPPER(rn) LIKE 'I%%')
                         OR (UPPER(prev_rn) LIKE 'V/V%%' AND UPPER(rn) LIKE 'I%%')
                       THEN 1
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
    except Exception:
        return

def _write_symbolic_onset_entropy_ts_bar(con, song_id: str, notes_df: pd.DataFrame, bars_df: pd.DataFrame, beat_subdiv: int = 4) -> None:
    """
    Groove proxy: entropy of onset positions on a 16th-note grid per bar (non-drum).
    Writes ts_bar.onset_entropy_16th in [0, log(K)].
    """
    if notes_df.empty or bars_df.empty:
        return
    bars = bars_df["bar"].astype(int).to_list()
    non_drum = notes_df.query("role != 'perc'").copy()
    if non_drum.empty:
        return
    rows = []
    for _, b in bars_df.iterrows():
        bar = int(b["bar"])
        in_bar = non_drum.loc[non_drum["onset_bar"] == bar]
        beats = float(b["num"]) * (4.0 / float(b["den"]))
        K = max(1, int(round(beats * beat_subdiv)))
        if in_bar.empty:
            rows.append((song_id, bar, "onset_entropy_16th", 0.0))
            continue
        # quantize onset_beat (1-based) into [0, K-1]
        pos = ((in_bar["onset_beat"].astype(float) - 1.0) * beat_subdiv).round().astype(int)
        pos = pos.clip(lower=0, upper=K - 1)
        h = np.bincount(pos.to_numpy(), minlength=K).astype(float)
        p = h / (h.sum() + 1e-9)
        ent = float(-(p * np.log(p + 1e-12)).sum())
        rows.append((song_id, bar, "onset_entropy_16th", ent))
    con.executemany(
        "INSERT OR REPLACE INTO ts_bar (song_id, bar, feature, value) VALUES (?, ?, ?, ?)",
        rows,
    )

def _write_windowed_key_regions(con, song_id: str, notes_df: pd.DataFrame, bars_df: pd.DataFrame, window_bars: int = 8) -> None:
    """
    Lightweight key-region tracker from symbolic chroma (Krumhansl-Schmuckler profiles).
    Writes sparse key_changes at region starts with confidence based on margin.
    """
    V = _symbolic_chroma_matrix(notes_df, bars_df)
    if V.shape[0] == 0:
        return
    bars = bars_df["bar"].astype(int).to_list()
    # Krumhansl major/minor profiles (normalized)
    maj = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88], dtype=float)
    minp = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17], dtype=float)
    maj = maj / maj.sum()
    minp = minp / minp.sum()
    profiles = []
    labels = []
    names = ["C", "C#", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"]
    for i in range(12):
        profiles.append(np.roll(maj, i))
        labels.append(f"{names[i]}:maj")
    for i in range(12):
        profiles.append(np.roll(minp, i))
        labels.append(f"{names[i]}:min")
    P = np.stack(profiles, axis=0)  # (24,12)

    # windowed scores
    preds: List[str] = []
    confs: List[float] = []
    for i in range(len(bars)):
        a = max(0, i - window_bars + 1)
        x = V[a : i + 1].sum(axis=0)
        if float(x.sum()) <= 0.0:
            preds.append("")
            confs.append(0.0)
            continue
        x = x / (x.sum() + 1e-9)
        scores = P @ x
        top2 = np.partition(scores, -2)[-2:]
        best = int(np.argmax(scores))
        margin = float(top2.max() - top2.min()) if len(top2) == 2 else 0.0
        preds.append(labels[best])
        confs.append(margin)

    # compress into regions
    regions: List[tuple] = []
    cur = None
    cur_start = None
    cur_conf = []
    for bar, k, c in zip(bars, preds, confs):
        if not k:
            continue
        if cur is None:
            cur = k
            cur_start = bar
            cur_conf = [c]
            continue
        if k == cur:
            cur_conf.append(c)
            continue
        regions.append((cur_start, cur, float(np.mean(cur_conf)) if cur_conf else 0.0))
        cur = k
        cur_start = bar
        cur_conf = [c]
    if cur is not None and cur_start is not None:
        regions.append((cur_start, cur, float(np.mean(cur_conf)) if cur_conf else 0.0))

    if not regions:
        return

    # Insert as key regions so this analysis does not override the global key used for RN labels.
    # Approximate end_bar as next region start-1 (or song end).
    last_bar = int(bars[-1]) if bars else 0
    rows = []
    for idx, (start_bar, key, conf) in enumerate(regions):
        if idx + 1 < len(regions):
            end_bar = int(regions[idx + 1][0]) - 1
        else:
            end_bar = last_bar
        if end_bar < int(start_bar):
            end_bar = int(start_bar)
        rows.append((song_id, int(start_bar), int(end_bar), key, float(min(1.0, max(0.0, conf)))))
    con.executemany(
        "INSERT OR REPLACE INTO key_regions (song_id, start_bar, end_bar, key, confidence) VALUES (?, ?, ?, ?, ?)",
        rows,
    )

def run(song_id: str, midi_path: str, con, cfg: SymbolicConfig = SymbolicConfig()):
    mtk = miditoolkit.MidiFile(midi_path)
    pm_obj = pm.PrettyMIDI(midi_path)

    # Tempo changes → tempo_changes
    try:
        t_sec, qpm = pm_obj.get_tempo_changes()
        if len(t_sec) > 0:
            tempo_df = pd.DataFrame({
                "song_id": song_id,
                "t_sec": t_sec.astype(float),
                "qpm": qpm.astype(float),
            })
            upsert_df(con, "tempo_changes", tempo_df)
    except Exception:
        pass

    # Time signatures → timesig_changes
    try:
        ts_rows = []
        for seg_tick, num, den in _ts_segments(mtk):
            ts_rows.append((song_id, float(pm_obj.tick_to_time(int(round(seg_tick)))), int(num), int(den)))
        if ts_rows:
            ts_df = pd.DataFrame(ts_rows, columns=["song_id","t_sec","num","den"])
            upsert_df(con, "timesig_changes", ts_df)
    except Exception:
        pass

    # Music21 parsing is expensive; reuse one parse for key and chord/RN inference.
    m21_stream = None
    k_est = None

    # Key estimate (symbolic) → key_changes (at bar 1)
    try:
        m21_stream = converter.parse(midi_path)
        k_est = m21_stream.analyze("Krumhansl")  # music21 key object
        root = getattr(k_est, "tonic", None)
        mode = getattr(k_est, "mode", None)
        if root is not None and mode is not None:
            root_name = str(root.name)
            mode_tag = "maj" if str(mode).lower().startswith("maj") else ("min" if str(mode).lower().startswith("min") else str(mode))
            key_str = f"{root_name}:{mode_tag}"
            key_df = pd.DataFrame([{ "song_id": song_id, "at_bar": 1, "at_beat": 1.0, "key": key_str, "confidence": 0.7 }])
            upsert_df(con, "key_changes", key_df)
    except Exception:
        m21_stream = None
        k_est = None

    # Bars (with per-bar tempo)
    bars_df = _build_bars(mtk, pm_obj)
    bars_df.insert(0, "song_id", song_id)
    # Only these columns exist in the bars table schema
    bars_df_db = bars_df[["song_id","bar","start_sec","end_sec","num","den","qpm"]].copy()

    # Tracks + heuristic roles
    tracks_rows = []
    for i, inst in enumerate(mtk.instruments):
        # mean pitch & monophony quick stats
        pitches = [n.pitch for n in inst.notes]
        mean_p = float(np.mean(pitches)) if pitches else 60.0
        is_mono = False
        if len(inst.notes) > 1:
            starts = sorted([n.start for n in inst.notes])
            is_mono = all(starts[i+1] >= inst.notes[i].end for i in range(len(inst.notes)-1))
        role = _gm_role(inst.is_drum, mean_p, is_mono)
        tracks_rows.append((song_id, f"t{i}", inst.name or f"Track {i}", inst.program, role))
    tracks_df = pd.DataFrame(tracks_rows, columns=["song_id","track_id","name","gm_program","role"])

    # Notes (+ dual time). Build once, then map ticks -> (bar, beat) vectorized.
    note_rows = []
    for i, inst in enumerate(mtk.instruments):
        tid = f"t{i}"
        for j, n in enumerate(inst.notes):
            start_tick = int(n.start)
            end_tick = int(n.end)
            onset_sec = pm_obj.tick_to_time(start_tick)
            offset_sec = pm_obj.tick_to_time(end_tick)
            note_rows.append((
                song_id,
                f"{tid}_n{j}",
                tid,
                int(n.pitch),
                int(n.velocity),
                float(onset_sec),
                float(offset_sec),
                float((end_tick - start_tick) / mtk.ticks_per_beat),
                start_tick,
                end_tick,
            ))
    notes_df = pd.DataFrame(note_rows, columns=[
        "song_id","note_id","track_id","pitch","velocity","onset_sec","offset_sec",
        "dur_beats","onset_tick","offset_tick"
    ])
    if not notes_df.empty:
        bar_arr, beat_arr = _map_ticks_to_bar_beat(notes_df["onset_tick"].to_numpy(dtype=np.int64), bars_df, mtk.ticks_per_beat)
        notes_df["onset_bar"] = bar_arr
        notes_df["onset_beat"] = beat_arr
    else:
        notes_df["onset_bar"] = []
        notes_df["onset_beat"] = []
    # join roles into notes (for metrics)
    notes_df = notes_df.merge(tracks_df[["song_id","track_id","role"]], on=["song_id","track_id"], how="left")

    # Chords + Romans (music21)
    chords_df = pd.DataFrame()
    try:
        if m21_stream is not None:
            chords_df = _infer_chords_romans(m21_stream, k_est=k_est)
    except Exception:
        chords_df = pd.DataFrame()
    if not chords_df.empty:
        chords_df.insert(0, "song_id", song_id)
        chords_df["section_id"] = None

    # Bar metrics
    poly = _polyphony_curve(notes_df, bars_df, cfg.beat_subdiv)
    backbeat = _backbeat_strength(notes_df, bars_df, eps=0.2)
    syncop = _syncopation_index(notes_df, bars_df, eps=cfg.syncop_eps)
    # density = note-ons per bar (normalized by beats)
    dens = notes_df.groupby("onset_bar")["note_id"].count().reindex(bars_df.bar, fill_value=0)
    beats_per_bar = bars_df.eval("num * (4.0/den)").astype(float)
    bar_metrics_df = pd.DataFrame({
        "song_id": song_id,
        "bar": bars_df.bar.values,
        "density": (dens.values / (beats_per_bar.values + 1e-6)),
        "polyphony": poly.values,
        "backbeat_strength": backbeat.values,
        "syncopation": syncop.values,
        "velocity_mean": notes_df.groupby("onset_bar")["velocity"].mean().reindex(bars_df.bar, fill_value=np.nan).values,
        "velocity_std":  notes_df.groupby("onset_bar")["velocity"].std().reindex(bars_df.bar, fill_value=np.nan).values,
    })

    # Very simple section draft: split every 8 bars (placeholder, refined later)
    sec_rows=[]
    if len(bars_df):
        for i in range(0, int(bars_df.bar.max()), 8):
            b0 = i+1
            b1 = min(i+8, int(bars_df.bar.max()))
            s_start = float(bars_df.loc[bars_df.bar==b0, "start_sec"].values[0])
            s_end   = float(bars_df.loc[bars_df.bar==b1, "end_sec"].values[0])
            sec_rows.append((song_id, f"S{i//8+1}", "other", b0, b1, s_start, s_end, "symbolic", 0.5))
    sections_df = pd.DataFrame(sec_rows, columns=[
        "song_id","section_id","type","start_bar","end_bar","start_sec","end_sec","source","confidence"
    ])

    # Motifs (top n)
    motifs_df = _top_motifs(notes_df, cfg)
    if not motifs_df.empty:
        motifs_df.insert(0, "song_id", song_id)

    # Write tables
    # Song-level metadata (for cost normalization and traceability)
    try:
        dur_sec = float(pm_obj.get_end_time()) if pm_obj is not None else None
    except Exception:
        dur_sec = None
    songs_df = pd.DataFrame([{
        "song_id": song_id,
        "title": song_id,
        "ppq": int(getattr(mtk, "ticks_per_beat", 0) or 0),
        "duration_sec": dur_sec,
    }])
    upsert_df(con, "songs", songs_df)
    upsert_df(con, "bars", bars_df_db)
    upsert_df(con, "tracks", tracks_df)
    # Only the schema columns exist in notes table (keep ticks in-memory only)
    notes_df_db = notes_df[[
        "song_id","note_id","track_id","pitch","velocity",
        "onset_sec","offset_sec","onset_bar","onset_beat","dur_beats"
    ]].copy()
    upsert_df(con, "notes", notes_df_db)
    if not chords_df.empty: upsert_df(con, "chords", chords_df)
    upsert_df(con, "bar_metrics", bar_metrics_df)
    # Also mirror into ts_bar so captions can consume without audio
    _mirror_bar_metrics_to_ts_bar(con, bar_metrics_df)
    # Symbolic substitutes for audio-derived structure features (supports section merging without rendering)
    _write_symbolic_structure_ts_bar(con, song_id, notes_df, bars_df)
    # MIDI-derived tags (defensible without audio synthesis)
    _write_symbolic_tags_section(con, song_id, tracks_df, bars_df, bar_metrics_df)
    # Additional MIDI-only musical understanding features
    _write_symbolic_role_activity_ts_bar(con, song_id, notes_df, tracks_df, bars_df)
    _write_symbolic_pitch_texture_ts_bar(con, song_id, notes_df, bars_df)
    _write_symbolic_onset_entropy_ts_bar(con, song_id, notes_df, bars_df, beat_subdiv=cfg.beat_subdiv)
    _write_windowed_key_regions(con, song_id, notes_df, bars_df, window_bars=8)
    _write_symbolic_cadence_ts_and_events(con, song_id)
    if not sections_df.empty: upsert_df(con, "sections", sections_df)
    if not motifs_df.empty: upsert_df(con, "motifs", motifs_df)
