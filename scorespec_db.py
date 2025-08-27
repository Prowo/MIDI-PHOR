from __future__ import annotations
from typing import Dict, Any, List

import duckdb


def _safe_int_track_id(tid: str) -> int | str:
    try:
        if isinstance(tid, str) and tid.startswith("t"):
            return int(tid[1:])
        return int(tid)
    except Exception:
        return tid


def build_scorespec(con: duckdb.DuckDBPyConnection, song_id: str) -> Dict[str, Any]:
    # Global
    meter_row = con.execute(
        """
        SELECT num, den FROM timesig_changes
        WHERE song_id=? ORDER BY t_sec LIMIT 1
        """,
        [song_id],
    ).fetchone()
    meter = f"{meter_row[0]}/{meter_row[1]}" if meter_row else None
    bpm_row = con.execute(
        "SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?",
        [song_id],
    ).fetchone()
    approx_bpm = int(bpm_row[0]) if bpm_row and bpm_row[0] is not None else None

    # Sections (merged preferred; fallback to symbolic)
    seg_rows = con.execute(
        """
        SELECT section_id, start_bar, end_bar
        FROM sections
        WHERE song_id=? AND source='merged'
        ORDER BY start_bar
        """,
        [song_id],
    ).fetchall()
    if not seg_rows:
        seg_rows = con.execute(
            """
            SELECT section_id, start_bar, end_bar
            FROM sections
            WHERE song_id=?
            ORDER BY start_bar
            """,
            [song_id],
        ).fetchall()
    segments = [
        {"id": sid, "bars": [int(sb), int(eb)]}
        for sid, sb, eb in seg_rows
    ]

    # Instruments (tracks)
    inst_rows = con.execute(
        """
        SELECT track_id, gm_program, name, COALESCE(role,'other')
        FROM tracks WHERE song_id=? ORDER BY track_id
        """,
        [song_id],
    ).fetchall()
    instruments = [
        {
            "track_id": _safe_int_track_id(tid),
            "program": int(gm if gm is not None else -1),
            "name": name,
            "role": role,
        }
        for tid, gm, name, role in inst_rows
    ]

    # Pitch-class spans (from chords)
    ch_rows = con.execute(
        """
        SELECT onset_bar, COALESCE(dur_beats,1.0) AS dur_beats, pcset
        FROM chords WHERE song_id=? ORDER BY onset_bar, onset_beat
        """,
        [song_id],
    ).fetchall()
    pitch_spans: List[Dict[str, Any]] = []
    for ob, dur, pcset in ch_rows:
        pcs: List[int] = []
        if pcset:
            try:
                s = str(pcset).strip("{}")
                if s:
                    pcs = [int(x) for x in s.split(",")]
            except Exception:
                pcs = []
        # crude span width: at least 1 bar
        span = {"bars": [int(ob), int(ob)], "pcs": pcs}
        pitch_spans.append(span)

    # Graph
    gn_rows = con.execute(
        """
        SELECT node_id, node_type, role, family
        FROM graph_nodes WHERE song_id=?
        """,
        [song_id],
    ).fetchall()
    nodes = [
        {
            "id": nid,
            "type": (nt or "unknown"),
            "payload": {"role": role, "family": fam},
        }
        for nid, nt, role, fam in gn_rows
    ]
    ge_rows = con.execute(
        """
        SELECT e.edge_id, e.src_node_id, e.dst_node_id, e.rel_type,
               MIN(ev.start_bar) AS sb, MAX(ev.end_bar) AS eb
        FROM graph_edges e
        LEFT JOIN edge_evidence ev USING (song_id, edge_id)
        WHERE e.song_id=?
        GROUP BY e.edge_id, e.src_node_id, e.dst_node_id, e.rel_type
        """,
        [song_id],
    ).fetchall()
    def _map_rel(r: str) -> str:
        m = (r or "").lower()
        if m == "doubles":
            return "DOUBLES"
        if m == "cooccur":
            return "OCCURS_IN"
        if m == "rhythmic_lock":
            return "RHYTHMIC_LOCK"
        return m.upper()

    edges = [
        {
            "src": src,
            "dst": dst,
            "rel": _map_rel(rel),
            "start_bar": (int(sb) if sb is not None else None),
            "end_bar": (int(eb) if eb is not None else None),
            "props": {},
        }
        for _, src, dst, rel, sb, eb in ge_rows
    ]

    # Motifs (optional)
    mot_rows = con.execute(
        """
        SELECT motif_id, pattern, occurrences, support
        FROM motifs WHERE song_id=?
        """,
        [song_id],
    ).fetchall()
    motifs = []
    import json as _json
    for mid, pattern, occ_json, support in mot_rows:
        occs = []
        try:
            arr = _json.loads(occ_json) if occ_json else []
            for o in arr:
                b = int(o.get("bar", 0))
                occs.append({"bar": b})
        except Exception:
            pass
        motifs.append({
            "id": str(mid),
            "pattern": str(pattern or ""),
            "occurrences": occs,
            "support": int(support or 0),
        })

    return {
        "file_id": song_id,
        "global": {"meter": meter, "approx_bpm": approx_bpm},
        "segments": segments,
        "instruments": instruments,
        "pitch_class_spans": pitch_spans,
        "graph": {"nodes": nodes, "edges": edges},
        "motifs": motifs,
    }


