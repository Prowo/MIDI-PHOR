# assemble/section_merge.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
import duckdb
import numpy as np

@dataclass
class MergeConfig:
    min_section_bars: int = 8                # shrink small segments into neighbors
    novelty_feature: str = "novelty_bar"     # bar-level novelty feature in ts_bar
    novelty_peak_z: float = 1.0              # z-threshold to accept a peak as a boundary
    absorb_tail_smaller_than: int = 4        # merge tiny trailing piece
    keep_existing_types: bool = True         # preserve 'type' from source sections when possible

def _song_last_bar(con: duckdb.DuckDBPyConnection, song_id: str) -> int:
    row = con.execute("SELECT MAX(bar) FROM bars WHERE song_id=?", [song_id]).fetchone()
    return int(row[0] or 0)

def _existing_boundaries(con: duckdb.DuckDBPyConnection, song_id: str) -> List[int]:
    """Collect boundaries from any pre-existing sections (symbolic/audio)."""
    rows = con.execute("""
        SELECT start_bar, end_bar + 1 AS end_next
        FROM sections
        WHERE song_id=? AND source IN ('symbolic','audio')
    """, [song_id]).fetchall()
    b = []
    for sb, enext in rows:
        if sb is not None: b.append(int(sb))
        if enext is not None: b.append(int(enext))
    return b

def _novelty_boundaries(con: duckdb.DuckDBPyConnection, song_id: str, cfg: MergeConfig) -> List[int]:
    """Detect boundaries from novelty peaks (bar-synchronous)."""
    # Ensure we have novelty z-scores; if not, compute them ad hoc
    # 1) check presence
    cnt = con.execute("""
        SELECT COUNT(*) FROM ts_bar WHERE song_id=? AND feature=?
    """, [song_id, cfg.novelty_feature]).fetchone()[0]
    if not cnt:
        return []

    # compute z into a temp view (safe, no writes)
    con.execute("""
        CREATE TEMP VIEW IF NOT EXISTS __nov_base AS
        SELECT bar, value
        FROM ts_bar WHERE song_id=? AND feature=?
    """, [song_id, cfg.novelty_feature])

    # peak picking: simple 1-bar neighborhood and z-threshold
    rows = con.execute("""
        WITH z AS (
          SELECT
            bar,
            value,
            (value - AVG(value) OVER()) / NULLIF(stddev_samp(value) OVER(), 0) AS z
          FROM __nov_base
        ),
        pk AS (
          SELECT
            bar, z,
            (value > LAG(value) OVER (ORDER BY bar)) AND
            (value >= LEAD(value) OVER (ORDER BY bar)) AS is_peak
          FROM z
        )
        SELECT bar FROM pk WHERE is_peak AND z >= ?
        ORDER BY bar
    """, [cfg.novelty_peak_z]).fetchall()

    return [int(b) for (b,) in rows]

def _clean_and_compose(boundaries: List[int], last_bar: int, cfg: MergeConfig) -> List[Tuple[int,int]]:
    """
    Turn boundary bars into [start_bar, end_bar] (inclusive) segments.
    Boundaries are 1-based bar indices; we also add sentinel last_bar+1.
    """
    if last_bar <= 0:
        return []
    # Normalize
    uniq = sorted({b for b in boundaries if 1 <= b <= last_bar+1})
    if 1 not in uniq: uniq = [1] + uniq
    if (last_bar + 1) not in uniq: uniq.append(last_bar + 1)

    # Build raw segments [sb, eb]
    segs = []
    for a, b in zip(uniq[:-1], uniq[1:]):
        sb, eb_excl = int(a), int(b)
        if eb_excl <= sb:
            continue
        segs.append([sb, eb_excl - 1])

    # Merge too-short segments
    i = 0
    while i < len(segs):
        length = segs[i][1] - segs[i][0] + 1
        if length < cfg.min_section_bars and len(segs) > 1:
            # merge with the neighbor having closer size after merge
            if i == 0:
                # merge forward
                segs[i+1][0] = segs[i][0]
                segs.pop(i)
                continue
            else:
                # merge backward
                segs[i-1][1] = segs[i][1]
                segs.pop(i)
                i -= 1
                continue
        i += 1

    # absorb tiny trailing piece
    if segs and (segs[-1][1] - segs[-1][0] + 1) < cfg.absorb_tail_smaller_than and len(segs) > 1:
        segs[-2][1] = segs[-1][1]
        segs.pop()

    return [(sb, eb) for sb, eb in segs]

def _fetch_existing_types(con: duckdb.DuckDBPyConnection, song_id: str) -> Dict[Tuple[int,int], str]:
    rows = con.execute("""
        SELECT start_bar, end_bar, type
        FROM sections
        WHERE song_id=? AND source IN ('symbolic','audio')
    """, [song_id]).fetchall()
    return {(int(sb), int(eb)): (t or "other") for sb, eb, t in rows}

def _bar_times(con: duckdb.DuckDBPyConnection, song_id: str, sb: int, eb: int) -> Tuple[float,float]:
    r1 = con.execute("SELECT start_sec FROM bars WHERE song_id=? AND bar=?", [song_id, sb]).fetchone()
    r2 = con.execute("SELECT end_sec FROM bars WHERE song_id=? AND bar=?", [song_id, eb]).fetchone()
    return float(r1[0]) if r1 else 0.0, float(r2[0]) if r2 else 0.0

def merge_for_song(con: duckdb.DuckDBPyConnection, song_id: str, cfg: MergeConfig = MergeConfig()) -> List[Tuple[str,int,int]]:
    """
    Build and write canonical merged sections for song_id.
    Returns list of (section_id, start_bar, end_bar).
    """
    last_bar = _song_last_bar(con, song_id)
    if last_bar <= 0:
        return []

    # gather boundaries
    b_existing = _existing_boundaries(con, song_id)
    b_novelty  = _novelty_boundaries(con, song_id, cfg)
    boundaries = sorted(set([1, last_bar + 1] + b_existing + b_novelty))

    # compose segments
    segs = _clean_and_compose(boundaries, last_bar, cfg)

    # pull existing type hints
    type_hints = _fetch_existing_types(con, song_id) if cfg.keep_existing_types else {}

    # clear previous merged
    con.execute("DELETE FROM sections WHERE song_id=? AND source='merged'", [song_id])

    out: List[Tuple[str,int,int]] = []
    for i, (sb, eb) in enumerate(segs, start=1):
        # choose type if we have an exact match from any source; else 'other'
        t = type_hints.get((sb, eb), "other")
        s_sec, e_sec = _bar_times(con, song_id, sb, eb)
        # Use 'M' prefix to avoid clashing with symbolic/audio section_ids
        sec_id = f"M{i}"
        con.execute("""
            INSERT INTO sections (song_id, section_id, type, start_bar, end_bar, start_sec, end_sec, source, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'merged', ?)
        """, [song_id, sec_id, t, sb, eb, s_sec, e_sec, 0.8 if (sb, eb) in type_hints else 0.6])
        out.append((sec_id, sb, eb))

    return out
