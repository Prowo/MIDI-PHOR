# utils/timing.py
from __future__ import annotations
from typing import Tuple, Optional, List
import math
import duckdb

# ---------- DB-based helpers ----------

def beats_per_bar(num: int, den: int) -> float:
    return num * (4.0 / den)

def sec_to_bar(con: duckdb.DuckDBPyConnection, song_id: str, t_sec: float) -> Optional[int]:
    row = con.execute("""
        SELECT bar FROM bars
        WHERE song_id=? AND start_sec <= ? AND end_sec > ?
        ORDER BY bar LIMIT 1
    """, [song_id, t_sec, t_sec]).fetchone()
    return int(row[0]) if row else None

def bar_to_sec_range(con: duckdb.DuckDBPyConnection, song_id: str, bar: int) -> Optional[Tuple[float,float]]:
    row = con.execute("SELECT start_sec, end_sec FROM bars WHERE song_id=? AND bar=?",
                      [song_id, int(bar)]).fetchone()
    if not row: return None
    return float(row[0]), float(row[1])

def barbeat_to_sec(con: duckdb.DuckDBPyConnection, song_id: str, bar: int, beat_in_bar: float) -> Optional[float]:
    """
    Convert (bar, beat) (1-based beat) to absolute seconds by linear interpolation within the bar.
    """
    row = con.execute("SELECT start_sec, end_sec, num, den FROM bars WHERE song_id=? AND bar=?",
                      [song_id, int(bar)]).fetchone()
    if not row: return None
    start_sec, end_sec, num, den = float(row[0]), float(row[1]), int(row[2]), int(row[3])
    bpb = beats_per_bar(num, den)
    # clamp beat to [1, bpb]
    beat = max(1.0, min(float(beat_in_bar), float(bpb)))
    frac = (beat - 1.0) / max(bpb, 1e-9)
    return start_sec + frac * (end_sec - start_sec)

# ---------- MIDI-segment helpers (optional) ----------
# Use these during ingest to build bars from time signatures and map ticks → (bar, beat).

def build_bars_df(mtk, pm_obj):
    """
    Create a pandas DataFrame: [bar, start_sec, end_sec, num, den, qpm]
    mtk: miditoolkit.MidiFile
    pm_obj: pretty_midi.PrettyMIDI
    """
    import pandas as pd
    def _segments():
        ts = sorted(mtk.time_signature_changes, key=lambda t: t.time)
        if not ts:
            return [(0, 4, 4)]
        return [(c.time, c.numerator, c.denominator) for c in ts]

    segs = _segments()
    last_tick = max((n.end for inst in mtk.instruments for n in inst.notes), default=0)
    tpb = mtk.ticks_per_beat
    rows = []
    bar_no = 1

    # tempo lookup via pretty_midi
    times, tempi = pm_obj.get_tempo_changes()
    def _qpm_at(sec: float) -> float:
        import numpy as np
        idx = int(np.searchsorted(times, sec, side="right") - 1)
        idx = max(0, min(idx, len(tempi) - 1))
        return float(tempi[idx])

    for i, (seg_tick, num, den) in enumerate(segs):
        seg_end = segs[i+1][0] if i+1 < len(segs) else last_tick
        bpb = beats_per_bar(num, den)
        tpb_bar = tpb * bpb
        tick = seg_tick
        while tick < seg_end + 1:
            start_sec = pm_obj.tick_to_time(tick)
            end_tick = min(tick + tpb_bar, seg_end)
            end_sec = pm_obj.tick_to_time(end_tick)
            qpm = _qpm_at(start_sec)
            rows.append((bar_no, start_sec, end_sec, num, den, qpm))
            bar_no += 1
            tick += tpb_bar

    return pd.DataFrame(rows, columns=["bar","start_sec","end_sec","num","den","qpm"])

def tick_to_bar_beat(tick: int, mtk, bars_df) -> Tuple[int, float]:
    """
    Map an absolute tick to (bar, beat_in_bar) using bars_df returned by build_bars_df.
    """
    tpb = mtk.ticks_per_beat

    # Build time-signature segments
    segs = sorted([(c.time, c.numerator, c.denominator) for c in mtk.time_signature_changes],
                  key=lambda x: x[0])
    if not segs:
        segs = [(0, 4, 4)]

    acc_bars = 1
    for i, (seg_tick, num, den) in enumerate(segs):
        seg_end = segs[i+1][0] if i+1 < len(segs) else 10**12
        bpb = num * (4.0 / den)
        tpb_bar = tpb * bpb
        if tick < seg_end:
            bars_since = math.floor((tick - seg_tick) / tpb_bar + 1e-9)
            bar = acc_bars + bars_since
            within = (tick - seg_tick) - bars_since * tpb_bar
            beat = within / tpb + 1.0
            return int(bar), float(beat)
        # advance bar counter
        seg_bars = math.floor((seg_end - seg_tick) / tpb_bar + 1e-9)
        acc_bars += seg_bars

    # fallback (shouldn't hit)
    last_bar = int(bars_df["bar"].max())
    return last_bar, 1.0
