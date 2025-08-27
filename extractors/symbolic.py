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
            rows.append((bar_no, start_sec, end_sec, num, den, qpm))
            bar_no += 1
            tick += ticks_per_bar

    df = pd.DataFrame(rows, columns=["bar","start_sec","end_sec","num","den","qpm"])
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
    # sample active-note count at 16th notes within each bar
    out = []
    for _, b in bars_df.iterrows():
        beats = b["num"] * (4.0/b["den"])
        steps = max(1, int(beats * beat_subdiv))
        ts = np.linspace(b.start_sec, b.end_sec, steps, endpoint=False)
        active = []
        for t in ts:
            # count notes active (onset_sec <= t < offset_sec)
            m = (notes_df.onset_sec <= t) & (notes_df.offset_sec > t)
            active.append(int(m.sum()))
        out.append(np.mean(active) if active else 0.0)
    return pd.Series(out, index=bars_df.bar.values, dtype=float)

def _backbeat_strength(notes_df: pd.DataFrame, bars_df: pd.DataFrame, eps=0.2) -> pd.Series:
    # Only for 4/4 bars; sum drum velocities near beats 2 and 4 / total drum velocity
    bb = []
    drum = notes_df.query("role == 'perc'").copy()
    for _, b in bars_df.iterrows():
        if not (b.num == 4 and b.den == 4):
            bb.append(np.nan); continue
        in_bar = drum.query("@b.bar <= onset_bar <= @b.bar")
        if in_bar.empty: bb.append(0.0); continue
        # distance to 2 or 4
        d2 = np.abs(in_bar.onset_beat - 2.0) <= eps
        d4 = np.abs(in_bar.onset_beat - 4.0) <= eps
        hits = in_bar.loc[d2 | d4, "velocity"].sum()
        tot  = in_bar["velocity"].sum() + 1e-6
        bb.append(float(hits / tot))
    return pd.Series(bb, index=bars_df.bar.values, dtype=float)

def _syncopation_index(notes_df: pd.DataFrame, bars_df: pd.DataFrame, eps=0.15) -> pd.Series:
    # Ratio of non-integer-beat onsets (weighted by velocity)
    out = []
    non_drum = notes_df.query("role != 'perc'")
    for _, b in bars_df.iterrows():
        in_bar = non_drum.query("@b.bar <= onset_bar <= @b.bar")
        if in_bar.empty: out.append(0.0); continue
        frac = np.abs(in_bar.onset_beat - np.round(in_bar.onset_beat))
        off = in_bar.loc[frac > eps, "velocity"].sum()
        tot = in_bar["velocity"].sum() + 1e-6
        out.append(float(off / tot))
    return pd.Series(out, index=bars_df.bar.values, dtype=float)

def _infer_chords_romans(midi_path: str) -> pd.DataFrame:
    s = converter.parse(midi_path)
    k = s.analyze("Krumhansl")
    seq = []
    ch = s.chordify()
    for c in ch.recurse().getElementsByClass("Chord"):
        if len({p.pitchClass for p in c.pitches}) < 2:  # skip singletons
            continue
        c = c.closedPosition(forceOctave=4)
        rn = roman.romanNumeralFromChord(c, k).figure
        # measureNumber and beat are available on chord
        bar = getattr(c, "measureNumber", None)
        beat = float(getattr(c, "beat", 1.0))
        dur_beats = float(getattr(c, "quarterLength", 1.0))
        name = c.pitchedCommonName  # e.g., 'C major triad'
        root_pc = int(c.root().pitchClass)
        quality = rn[0] if rn else "other"
        chord_id = str(uuid.uuid4())[:12]
        seq.append((chord_id, bar, beat, dur_beats, name, rn, root_pc, quality, "{"+",".join(map(str,sorted({p.pitchClass for p in c.pitches})))+"}"))
    df = pd.DataFrame(seq, columns=["chord_id","onset_bar","onset_beat","dur_beats","name","rn","root_pc","quality","pcset"])
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

    # Key estimate (symbolic) → key_changes (at bar 1)
    try:
        s = converter.parse(midi_path)
        k_est = s.analyze("Krumhansl")  # music21 key object
        root = getattr(k_est, "tonic", None)
        mode = getattr(k_est, "mode", None)
        if root is not None and mode is not None:
            root_name = str(root.name)
            mode_tag = "maj" if str(mode).lower().startswith("maj") else ("min" if str(mode).lower().startswith("min") else str(mode))
            key_str = f"{root_name}:{mode_tag}"
            key_df = pd.DataFrame([{ "song_id": song_id, "at_bar": 1, "at_beat": 1.0, "key": key_str, "confidence": 0.7 }])
            upsert_df(con, "key_changes", key_df)
    except Exception:
        pass

    # Bars (with per-bar tempo)
    bars_df = _build_bars(mtk, pm_obj)
    bars_df.insert(0, "song_id", song_id)

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

    # Notes (+ dual time)
    note_rows = []
    for i, inst in enumerate(mtk.instruments):
        tid = f"t{i}"
        for j, n in enumerate(inst.notes):
            onset_sec = pm_obj.tick_to_time(n.start)
            offset_sec = pm_obj.tick_to_time(n.end)
            bar, beat = _tick_to_bar_beat(n.start, mtk, bars_df)
            note_rows.append((song_id, f"{tid}_n{j}", tid, n.pitch, n.velocity,
                              onset_sec, offset_sec, bar, float(beat),
                              (n.end - n.start)/mtk.ticks_per_beat))
    notes_df = pd.DataFrame(note_rows, columns=[
        "song_id","note_id","track_id","pitch","velocity","onset_sec","offset_sec",
        "onset_bar","onset_beat","dur_beats"
    ])
    # join roles into notes (for metrics)
    notes_df = notes_df.merge(tracks_df[["song_id","track_id","role"]], on=["song_id","track_id"], how="left")

    # Chords + Romans (music21)
    chords_df = _infer_chords_romans(midi_path)
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
    upsert_df(con, "bars", bars_df)
    upsert_df(con, "tracks", tracks_df)
    upsert_df(con, "notes", notes_df.drop(columns=["role"]))   # role lives in tracks
    if not chords_df.empty: upsert_df(con, "chords", chords_df)
    upsert_df(con, "bar_metrics", bar_metrics_df)
    # Also mirror into ts_bar so captions can consume without audio
    _mirror_bar_metrics_to_ts_bar(con, bar_metrics_df)
    if not sections_df.empty: upsert_df(con, "sections", sections_df)
    if not motifs_df.empty: upsert_df(con, "motifs", motifs_df)
