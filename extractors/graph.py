# extractors/graph.py
from __future__ import annotations
import json
from typing import List, Tuple
import duckdb

from utils.ids import deterministic_id

def _insert_nodes_sections(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    rows = con.execute("""
        SELECT section_id, type, start_bar, end_bar FROM sections
        WHERE song_id=? ORDER BY start_bar
    """, [song_id]).fetchall()
    for sid, typ, sb, eb in rows:
        payload = json.dumps({"type": typ or "other", "start_bar": int(sb), "end_bar": int(eb)})
        con.execute("""
            INSERT OR REPLACE INTO graph_nodes (song_id, node_id, node_type, track_id, role, family)
            VALUES (?, ?, 'role', NULL, ?, ?)
        """, [song_id, f"sec:{sid}", typ or "other", "section"])
        # We also keep a richer record in a payload-only node if you prefer:
        con.execute("""
            INSERT OR REPLACE INTO graph_nodes (song_id, node_id, node_type, track_id, role, family)
            VALUES (?, ?, 'track', NULL, ?, ?)
        """, [song_id, f"secmeta:{sid}", payload, "meta"])

def _insert_nodes_tracks(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    rows = con.execute("""
        SELECT track_id, name, role,
               COALESCE(role, 'other') AS family
        FROM tracks WHERE song_id=?
    """, [song_id]).fetchall()
    for tid, name, role, fam in rows:
        con.execute("""
            INSERT OR REPLACE INTO graph_nodes (song_id, node_id, node_type, track_id, role, family)
            VALUES (?, ?, 'track', ?, ?, ?)
        """, [song_id, f"trk:{tid}", str(tid), role or "other", fam or "other"])

def _edges_occurs_in(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    rows = con.execute("""
        SELECT DISTINCT n.track_id, s.section_id, s.start_bar, s.end_bar
        FROM notes n
        JOIN sections s ON s.song_id=n.song_id
        WHERE n.song_id=? AND n.onset_bar BETWEEN s.start_bar AND s.end_bar
    """, [song_id]).fetchall()
    for tid, sid, sb, eb in rows:
        src = f"trk:{tid}"; dst = f"sec:{sid}"
        edge_id = deterministic_id("e", [song_id, src, "OCCURS_IN", dst])
        con.execute("""
            INSERT OR REPLACE INTO graph_edges (song_id, edge_id, src_node_id, dst_node_id, rel_type, strength)
            VALUES (?, ?, ?, ?, 'cooccur', ?)
        """, [song_id, edge_id, src, dst, float(eb - sb + 1)])
        con.execute("""
            INSERT OR REPLACE INTO edge_evidence (song_id, edge_id, section_id, start_bar, end_bar, events, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [song_id, edge_id, sid, int(sb), int(eb), int(eb - sb + 1), 0.8])

def _edges_doubles(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    rows = con.execute("""
        SELECT n1.track_id AS t1, n2.track_id AS t2, n1.onset_bar AS bar, COUNT(*) AS c
        FROM notes n1
        JOIN notes n2
          ON n1.song_id=n2.song_id
         AND n1.onset_bar=n2.onset_bar
         AND n1.pitch=n2.pitch
         AND n1.track_id < n2.track_id
        WHERE n1.song_id=?
        GROUP BY 1,2,3
        HAVING c >= 3
        ORDER BY bar
    """, [song_id]).fetchall()
    # group by pairs into spans
    from collections import defaultdict
    spans = defaultdict(list)
    for t1, t2, bar, c in rows:
        spans[(t1,t2)].append((bar, c))
    for (t1,t2), lst in spans.items():
        src = f"trk:{t1}"; dst = f"trk:{t2}"
        edge_id = deterministic_id("e", [song_id, src, "doubles", dst])
        # evidence: coalesced into min/max
        start_bar = min(b for b,_ in lst); end_bar = max(b for b,_ in lst)
        strength = float(sum(c for _,c in lst))
        con.execute("""
            INSERT OR REPLACE INTO graph_edges (song_id, edge_id, src_node_id, dst_node_id, rel_type, strength)
            VALUES (?, ?, ?, ?, 'doubles', ?)
        """, [song_id, edge_id, src, dst, strength])
        con.execute("""
            INSERT OR REPLACE INTO edge_evidence (song_id, edge_id, section_id, start_bar, end_bar, events, confidence)
            VALUES (?, ?, NULL, ?, ?, ?, ?)
        """, [song_id, edge_id, int(start_bar), int(end_bar), int(strength), 0.7])

def _edges_rhythmic_lock(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    """
    Bass↔Drums rhythmic lock: same bar, many near-synchronous onsets.
    """
    rows = con.execute("""
        WITH fam AS (
          SELECT t.song_id, n.track_id, n.onset_bar AS bar, COUNT(*) AS onsets,
                 CASE
                   WHEN LOWER(COALESCE(t.role,'')) LIKE '%perc%' THEN 'drums'
                   WHEN LOWER(COALESCE(t.role,'')) LIKE '%bass%' THEN 'bass'
                   ELSE 'other'
                 END AS family
          FROM notes n JOIN tracks t USING(song_id, track_id)
          WHERE n.song_id=?
          GROUP BY t.song_id, n.track_id, bar, t.role
        ),
        pairs AS (
          SELECT d.bar, d.track_id AS drums, b.track_id AS bass,
                 LEAST(d.onsets, b.onsets) AS lock_score
          FROM fam d JOIN fam b USING (song_id, bar)
          WHERE d.family='drums' AND b.family='bass'
        )
        SELECT drums, bass, MIN(bar), MAX(bar), SUM(lock_score)
        FROM pairs
        GROUP BY drums, bass
        HAVING SUM(lock_score) >= 6
    """, [song_id]).fetchall()
    for drums, bass, sb, eb, tot in rows:
        src = f"trk:{bass}"; dst = f"trk:{drums}"
        edge_id = deterministic_id("e", [song_id, src, "rhythmic_lock", dst])
        con.execute("""
            INSERT OR REPLACE INTO graph_edges (song_id, edge_id, src_node_id, dst_node_id, rel_type, strength)
            VALUES (?, ?, ?, ?, 'rhythmic_lock', ?)
        """, [song_id, edge_id, src, dst, float(tot)])
        con.execute("""
            INSERT OR REPLACE INTO edge_evidence (song_id, edge_id, section_id, start_bar, end_bar, events, confidence)
            VALUES (?, ?, NULL, ?, ?, ?, ?)
        """, [song_id, edge_id, int(sb), int(eb), int(tot), 0.75])

def _node_activity(con: duckdb.DuckDBPyConnection, song_id: str) -> None:
    con.execute("""
        INSERT OR REPLACE INTO node_activity (song_id, node_id, start_bar, end_bar, active_ratio)
        SELECT n.song_id, 'trk:' || n.track_id, MIN(n.onset_bar), MAX(n.onset_bar), 1.0
        FROM notes n
        WHERE n.song_id=?
        GROUP BY n.song_id, n.track_id
    """, [song_id])

def run(song_id: str, con: duckdb.DuckDBPyConnection):
    _insert_nodes_tracks(con, song_id)
    _insert_nodes_sections(con, song_id)
    _edges_occurs_in(con, song_id)
    _edges_doubles(con, song_id)
    _edges_rhythmic_lock(con, song_id)
    _node_activity(con, song_id)
