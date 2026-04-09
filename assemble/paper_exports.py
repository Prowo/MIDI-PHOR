"""
Paper-aligned export bundle derived from the live DuckDB pipeline.

These are **approximations** of the ACL / MIDIPHOR paper artifacts (ScoreSpec family,
Enhanced Facts, Hierarchical Facts). The full historical ScoreSpec generator was removed
from the trimmed repo; this module reconstructs a consistent JSON/text surface from
tables populated by symbolic extraction, section merge, graph, and slots.

Not byte-identical to legacy `scorespec_json/` exports if those existed elsewhere.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from .slots import build_slots

SCORESPEC_DERIVED_VERSION = 1


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> List[tuple]:
    try:
        return con.execute(sql, params).fetchall()
    except Exception:
        return []


def _song_row(con: duckdb.DuckDBPyConnection, song_id: str) -> Dict[str, Any]:
    r = con.execute("SELECT song_id, title, ppq, duration_sec FROM songs WHERE song_id=?", [song_id]).fetchone()
    if not r:
        return {"song_id": song_id}
    return {"song_id": r[0], "title": r[1], "ppq": r[2], "duration_sec": r[3]}


def _pc_profile_for_span(
    con: duckdb.DuckDBPyConnection, song_id: str, lo: int, hi: int, top_k: int = 6
) -> Dict[str, Any]:
    rows = _rows(
        con,
        """
        SELECT (pitch % 12) AS pc, COUNT(*) AS c
        FROM notes
        WHERE song_id=? AND onset_bar BETWEEN ? AND ?
        GROUP BY 1 ORDER BY c DESC LIMIT ?
        """,
        [song_id, lo, hi, top_k],
    )
    return {str(int(pc)): int(c) for pc, c in rows}


def _active_tracks(con: duckdb.DuckDBPyConnection, song_id: str, lo: int, hi: int) -> List[Dict[str, Any]]:
    rows = _rows(
        con,
        """
        SELECT DISTINCT t.track_id, t.name, t.role, t.gm_program
        FROM tracks t
        JOIN notes n ON n.song_id = t.song_id AND n.track_id = t.track_id
        WHERE t.song_id = ? AND n.onset_bar BETWEEN ? AND ?
        ORDER BY t.track_id
        """,
        [song_id, lo, hi],
    )
    return [
        {"track_id": a, "name": b, "role": c, "gm_program": d}
        for a, b, c, d in rows
    ]


def _harmonic_snippet(con: duckdb.DuckDBPyConnection, song_id: str, lo: int, hi: int, n: int = 8) -> str:
    rows = _rows(
        con,
        """
        SELECT COALESCE(rn, name, '?') FROM chords
        WHERE song_id=? AND onset_bar BETWEEN ? AND ?
        ORDER BY onset_bar, onset_beat LIMIT ?
        """,
        [song_id, lo, hi, n],
    )
    seq = []
    for (x,) in rows:
        if not seq or seq[-1] != x:
            seq.append(x)
    return "–".join(seq) if seq else ""


def _avg_metrics(con: duckdb.DuckDBPyConnection, song_id: str, lo: int, hi: int) -> Tuple[Optional[float], Optional[float]]:
    d = con.execute(
        "SELECT AVG(density), AVG(polyphony) FROM bar_metrics WHERE song_id=? AND bar BETWEEN ? AND ?",
        [song_id, lo, hi],
    ).fetchone()
    if not d:
        return None, None
    return (float(d[0]) if d[0] is not None else None, float(d[1]) if d[1] is not None else None)


def _section_tempo(con: duckdb.DuckDBPyConnection, song_id: str, lo: int, hi: int) -> Optional[float]:
    r = con.execute(
        "SELECT AVG(qpm) FROM bars WHERE song_id=? AND bar BETWEEN ? AND ?",
        [song_id, lo, hi],
    ).fetchone()
    return float(r[0]) if r and r[0] is not None else None


def build_scorespec(con: duckdb.DuckDBPyConnection, song_id: str, structural_graph: Dict[str, Any]) -> Dict[str, Any]:
    """Fine-grained bundle: segments, instruments, pitch-class summaries, motifs, graph."""
    song = _song_row(con, song_id)
    seg_rows = _rows(
        con,
        """
        SELECT section_id, type, start_bar, end_bar, source, confidence
        FROM sections WHERE song_id=? ORDER BY start_bar, section_id
        """,
        [song_id],
    )
    segments = [
        {
            "section_id": a,
            "type": b,
            "start_bar": c,
            "end_bar": d,
            "source": e,
            "confidence": float(f) if f is not None else None,
        }
        for a, b, c, d, e, f in seg_rows
    ]

    tr = _rows(
        con,
        "SELECT track_id, name, gm_program, role FROM tracks WHERE song_id=? ORDER BY track_id",
        [song_id],
    )
    tracks = [
        {"track_id": a, "name": b, "gm_program": c, "role": d} for a, b, c, d in tr
    ]

    # Pitch-class profiles: whole piece + per section
    bars = con.execute("SELECT MIN(bar), MAX(bar) FROM bars WHERE song_id=?", [song_id]).fetchone()
    lo_g, hi_g = (int(bars[0]), int(bars[1])) if bars and bars[0] is not None else (1, 1)
    pitch_class_spans: List[Dict[str, Any]] = [
        {
            "span_label": "full_piece",
            "start_bar": lo_g,
            "end_bar": hi_g,
            "pitch_class_counts": _pc_profile_for_span(con, song_id, lo_g, hi_g, top_k=12),
        }
    ]
    for s in segments:
        lo, hi = int(s["start_bar"]), int(s["end_bar"])
        pitch_class_spans.append(
            {
                "span_label": f"section:{s['section_id']}",
                "section_type": s.get("type"),
                "start_bar": lo,
                "end_bar": hi,
                "pitch_class_counts": _pc_profile_for_span(con, song_id, lo, hi, top_k=12),
            }
        )

    motif_rows = _rows(
        con,
        "SELECT motif_id, pattern, occurrences, support FROM motifs WHERE song_id=? ORDER BY motif_id",
        [song_id],
    )
    motifs: List[Dict[str, Any]] = []
    for mid, pat, occ, sup in motif_rows:
        occ_l: List[Any] = []
        if occ:
            try:
                v = json.loads(occ)
                occ_l = v if isinstance(v, list) else []
            except Exception:
                occ_l = []
        motifs.append(
            {
                "motif_id": mid,
                "pattern": pat,
                "support": int(sup) if sup is not None else 0,
                "occurrences": occ_l,
            }
        )

    return {
        "format": "midiphor_scorespec_derived",
        "version": SCORESPEC_DERIVED_VERSION,
        "song": song,
        "segments": segments,
        "instruments": tracks,
        "pitch_class_spans": pitch_class_spans,
        "motif_occurrences": motifs,
        "structural_graph": {
            "nodes": structural_graph.get("nodes") or [],
            "edges": structural_graph.get("edges") or [],
        },
    }


def build_scorespec_lite(con: duckdb.DuckDBPyConnection, song_id: str) -> Dict[str, Any]:
    """Section-level summaries for lower token overhead."""
    seg_rows = _rows(
        con,
        """
        SELECT section_id, type, start_bar, end_bar FROM sections
        WHERE song_id=? ORDER BY start_bar, section_id
        """,
        [song_id],
    )
    if not seg_rows:
        bars = con.execute("SELECT MIN(bar), MAX(bar) FROM bars WHERE song_id=?", [song_id]).fetchone()
        if bars and bars[0] is not None:
            lo, hi = int(bars[0]), int(bars[1])
            seg_rows = [("whole_song", "other", lo, hi)]

    sections_out: List[Dict[str, Any]] = []
    for section_id, typ, sb, eb in seg_rows:
        lo, hi = int(sb), int(eb)
        dens, poly = _avg_metrics(con, song_id, lo, hi)
        sections_out.append(
            {
                "section_id": section_id,
                "type": typ,
                "bars": [lo, hi],
                "tempo_bpm_approx": _section_tempo(con, song_id, lo, hi),
                "density_mean": dens,
                "polyphony_mean": poly,
                "active_instruments": _active_tracks(con, song_id, lo, hi),
                "harmonic_summary": _harmonic_snippet(con, song_id, lo, hi),
            }
        )
    return {"format": "midiphor_scorespec_lite_derived", "version": SCORESPEC_DERIVED_VERSION, "sections": sections_out}


def build_enhanced_facts(con: duckdb.DuckDBPyConnection, song_id: str) -> str:
    """Bullet list from structured slots (prompt-injection style)."""
    s = build_slots(con, song_id, section_id=None)
    lines: List[str] = []
    lines.append(f"- Meter {s.get('meter', '?')} at ~{s.get('tempo_bpm') or '?'} BPM")
    lines.append(f"- Key / tonal frame: {s.get('key') or 'n/a'}")
    lines.append(f"- Bar span: {s['bars'][0]}–{s['bars'][1]}")
    lines.append(f"- Harmonic motion (Roman numerals): {s.get('progression') or 'n/a'}")
    if s.get("rhythm_trait"):
        lines.append(f"- Rhythm: {s['rhythm_trait']}")
    if s.get("texture_blurb"):
        lines.append(f"- Texture: {s['texture_blurb']}")
    ez, d, p = s.get("energy_z"), s.get("density"), s.get("polyphony")
    if ez is not None:
        lines.append(f"- Energy (bar z-score): {ez:.2f}")
    if d is not None:
        lines.append(f"- Note density (relative): {d:.2f}")
    if p is not None:
        lines.append(f"- Polyphony (relative): {p:.2f}")
    inst = s.get("instruments_summary") or []
    if inst:
        lines.append(f"- Active parts (summary): {', '.join(inst[:8])}")
    tags = s.get("tags") or []
    if tags:
        lines.append(f"- Tags: {', '.join(tags[:10])}")
    ev = s.get("events") or []
    if ev:
        bits = [f"bar {e.get('bar')} {e.get('type')} ({float(e.get('strength', 0)):.2f})" for e in ev[:6]]
        lines.append("- Salient events: " + "; ".join(bits))
    return "\n".join(lines) + "\n"


def build_hierarchical_facts(con: duckdb.DuckDBPyConnection, song_id: str) -> Dict[str, List[str]]:
    """Topic buckets for reporting / querying."""
    s = build_slots(con, song_id, section_id=None)
    structure: List[str] = []
    harmony: List[str] = []
    rhythm: List[str] = []
    orchestration: List[str] = []
    motifs_l: List[str] = []
    form: List[str] = []

    structure.append(f"Bars {s['bars'][0]}–{s['bars'][1]}; meter {s.get('meter', '?')}")
    if s.get("tempo_bpm"):
        structure.append(f"Tempo ~{s['tempo_bpm']} BPM")

    harmony.append(f"Key: {s.get('key') or 'n/a'}")
    harmony.append(f"Progression: {s.get('progression') or 'n/a'}")
    if s.get("harmonic_rhythm") is not None:
        harmony.append(f"Harmonic rhythm index: {float(s['harmonic_rhythm']):.2f}")

    rhythm.append(f"Rhythm trait: {s.get('rhythm_trait') or 'n/a'}")
    if s.get("backbeat_strength") is not None:
        rhythm.append(f"Backbeat strength: {float(s['backbeat_strength']):.2f}")
    if s.get("syncopation") is not None:
        rhythm.append(f"Syncopation: {float(s['syncopation']):.2f}")

    inst = s.get("instruments_summary") or []
    if inst:
        orchestration.append("Instruments: " + ", ".join(inst[:10]))
    if s.get("texture_blurb"):
        orchestration.append(f"Texture: {s['texture_blurb']}")

    mrows = _rows(con, "SELECT motif_id, support, pattern FROM motifs WHERE song_id=? LIMIT 8", [song_id])
    for mid, sup, pat in mrows:
        motifs_l.append(f"{mid} (support {sup}): {pat}")

    sec = _rows(
        con,
        "SELECT type, start_bar, end_bar FROM sections WHERE song_id=? ORDER BY start_bar LIMIT 12",
        [song_id],
    )
    for t, a, b in sec:
        form.append(f"{t} bars {a}–{b}")

    return {
        "structure": structure,
        "harmony": harmony,
        "rhythm": rhythm,
        "orchestration": orchestration,
        "motifs": motifs_l,
        "form": form,
    }


def build_paper_export_bundle(
    con: duckdb.DuckDBPyConnection, song_id: str, structural_graph: Dict[str, Any]
) -> Dict[str, Any]:
    scorespec = build_scorespec(con, song_id, structural_graph)
    lite = build_scorespec_lite(con, song_id)
    enhanced = build_enhanced_facts(con, song_id)
    hierarchical = build_hierarchical_facts(con, song_id)
    return {
        "scorespec": scorespec,
        "scorespec_lite": lite,
        "enhanced_facts": enhanced,
        "hierarchical_facts": hierarchical,
    }
