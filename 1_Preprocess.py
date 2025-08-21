#!/usr/bin/env python3
# build_text_dataset.py
# Objective-mode pipeline: MIDI -> DuckDB -> ScoreSpec JSON -> (LLM-ready) long description

import argparse, json, uuid
from pathlib import Path
from typing import List, Tuple, Optional

import duckdb
import numpy as np
import pretty_midi
import mido


# ----------------------------
# 0) DB: schema & connector
# ----------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
  file_id TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  title TEXT,
  duration_s DOUBLE,
  ppq INTEGER
);
CREATE TABLE IF NOT EXISTS tracks (
  file_id TEXT,
  track_id INTEGER,
  program INTEGER,
  is_drum BOOLEAN,
  name TEXT,
  PRIMARY KEY (file_id, track_id)
);
CREATE TABLE IF NOT EXISTS notes (
  file_id TEXT,
  track_id INTEGER,
  note_id INTEGER,
  start_s DOUBLE,
  end_s DOUBLE,
  start_tick BIGINT,
  end_tick BIGINT,
  bar INTEGER,
  beat DOUBLE,
  pitch INTEGER,
  velocity INTEGER,
  PRIMARY KEY (file_id, track_id, note_id)
);
CREATE TABLE IF NOT EXISTS controllers (
  file_id TEXT,
  track_id INTEGER,
  cc INTEGER,
  time_s DOUBLE,
  tick BIGINT,
  value INTEGER
);
CREATE TABLE IF NOT EXISTS tempo_ts (
  file_id TEXT,
  time_s DOUBLE,
  bpm DOUBLE
);
CREATE TABLE IF NOT EXISTS tsigs (
  file_id TEXT,
  time_s DOUBLE,
  num INTEGER,
  den INTEGER
);
-- Optional/heuristic: if you want absolute zero subjectivity, you can skip 'keys'
CREATE TABLE IF NOT EXISTS keys (
  file_id TEXT,
  time_s DOUBLE,
  key TEXT
);

-- Symbolic (objective-mode)
CREATE TABLE IF NOT EXISTS sections ( -- neutral "segments"
  file_id TEXT,
  section_id TEXT,
  name TEXT,
  start_bar INTEGER,
  end_bar INTEGER
);

CREATE TABLE IF NOT EXISTS pitch_class_spans (
  file_id TEXT,
  span_id TEXT,
  start_bar INTEGER,
  end_bar INTEGER,
  pcs TEXT -- JSON string of sorted pitch-class list, e.g. "[0,4,7]"
);

CREATE TABLE IF NOT EXISTS ensemble (
  file_id TEXT,
  track_id INTEGER,
  register_low INTEGER,
  register_high INTEGER,
  enter_bar INTEGER,
  exit_bar INTEGER
);

CREATE TABLE IF NOT EXISTS layering (
  file_id TEXT,
  event_id TEXT,
  type TEXT,      -- enter, exit
  bar INTEGER,
  track_id INTEGER
);

CREATE TABLE IF NOT EXISTS motifs (
  file_id TEXT,
  motif_id TEXT,
  pattern_repr TEXT,
  n_occ INTEGER
);

CREATE TABLE IF NOT EXISTS motif_occ (
  file_id TEXT,
  motif_id TEXT,
  track_id INTEGER,
  start_bar INTEGER,
  end_bar INTEGER
);

-- Text & final descriptions
CREATE TABLE IF NOT EXISTS facts_text (
  file_id TEXT,
  fact_id TEXT,
  start_bar INTEGER,
  end_bar INTEGER,
  score_path TEXT,
  text TEXT
);

CREATE TABLE IF NOT EXISTS descriptions (
  file_id TEXT PRIMARY KEY,
  description TEXT,
  claims JSON   -- array of {text, evidence:[pointers]}
);

-- Graph tables
CREATE TABLE IF NOT EXISTS graph_nodes (
  file_id TEXT,
  node_id TEXT,
  type TEXT,  -- Section, Track, ChordSpan, Motif, MotifOcc, TextureEvent, ControllerSummary
  ref_key TEXT, -- e.g., section_id, track_id, span_id, motif_id, event_id
  payload JSON
);

CREATE TABLE IF NOT EXISTS graph_edges (
  file_id TEXT,
  src TEXT,
  rel TEXT,  -- OCCURS_IN, DOUBLES, CALLS, ANSWERS, SUPPORTS_HARMONY_OF, CONTROLS, FOLLOWS, PLAYED_BY
  dst TEXT,
  start_bar INTEGER,
  end_bar INTEGER,
  props JSON
);
"""

def connect_db(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    return con


# ----------------------------
# 1) MIDI -> time-series
# ----------------------------

def estimate_bar_and_beat(pm: pretty_midi.PrettyMIDI, t: float, beats: np.ndarray, beats_per_bar: int = 4) -> Tuple[int, float]:
    """Approximate bar/beat with a simple 4/4 grid. Replace with ts-aware logic later."""
    if len(beats) == 0:
        return 0, 0.0
    i = np.searchsorted(beats, t, side="right") - 1
    i = max(0, min(i, len(beats) - 1))
    next_i = min(i + 1, len(beats) - 1)
    frac = 0.0 if next_i == i else (t - beats[i]) / max(beats[next_i] - beats[i], 1e-9)
    bar = int(i // beats_per_bar)
    beat_in_bar = float((i % beats_per_bar) + frac)
    return bar, beat_in_bar

def ingest_midi(con: duckdb.DuckDBPyConnection, file_id: str, path: Path) -> None:
    pm = pretty_midi.PrettyMIDI(str(path))
    mid = mido.MidiFile(str(path))

    # files
    con.execute("INSERT INTO files VALUES (?, ?, ?, ?, ?)",
                [file_id, str(path), path.stem, pm.get_end_time(), pm.resolution])

    # tempo changes
    times, tempi = pm.get_tempo_changes()  # seconds, BPM
    if len(times) == 0:
        times = np.array([0.0])
        tempi = np.array([120.0])
    con.executemany("INSERT INTO tempo_ts VALUES (?, ?, ?)",
                    [(file_id, float(t), float(b)) for t, b in zip(times, tempi)])

    # time signatures (optional; not used in this 4/4 approx)
    for ts in pm.time_signature_changes:
        con.execute("INSERT INTO tsigs VALUES (?, ?, ?, ?)",
                    [file_id, float(ts.time), int(ts.numerator), int(ts.denominator)])

    # tracks & notes
    beats = pm.get_beats()
    for tidx, inst in enumerate(pm.instruments):
        con.execute("INSERT INTO tracks VALUES (?, ?, ?, ?, ?)",
                    [file_id, tidx, int(inst.program), bool(inst.is_drum), inst.name or f"track_{tidx}"])
        for nidx, n in enumerate(inst.notes):
            bar, beat = estimate_bar_and_beat(pm, n.start, beats, beats_per_bar=4)
            con.execute("""
                INSERT INTO notes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [file_id, tidx, nidx, float(n.start), float(n.end), 0, 0, int(bar), float(beat), int(n.pitch), int(n.velocity)])

    # controllers (approximate seconds; refine with exact tempo map later)
    # Use channel as track proxy when track mapping is unavailable (objective-mode keeps it coarse)
    t_s = 0.0
    ticks_per_beat = mid.ticks_per_beat or 480
    # crude tempo: use first tempo (BPM) from pretty_midi
    bpm0 = float(tempi[0]) if len(tempi) else 120.0
    for msg in mid:
        t_s += mido.tick2second(msg.time, ticks_per_beat, 60.0 / bpm0 * 1e6)  # us per beat
        if msg.type == "control_change":
            con.execute("INSERT INTO controllers VALUES (?, ?, ?, ?, ?, ?)",
                        [file_id, msg.channel, int(msg.control), float(t_s), 0, int(msg.value)])
        elif msg.type == "pitchwheel":
            # store as pseudo-cc 8192
            con.execute("INSERT INTO controllers VALUES (?, ?, ?, ?, ?, ?)",
                        [file_id, msg.channel, 8192, float(t_s), 0, int(msg.pitch)])


# ----------------------------
# 2) Objective symbolic facts
# ----------------------------

def seed_segments(con: duckdb.DuckDBPyConnection, file_id: str, win_bars: int = 8) -> None:
    bars_total = con.execute("SELECT 1+MAX(bar) FROM notes WHERE file_id=?", [file_id]).fetchone()[0] or 0
    seg_id, bar = 0, 0
    while bar < bars_total:
        sb, eb = bar, min(bar + win_bars, bars_total)
        con.execute("""
            INSERT INTO sections (file_id, section_id, name, start_bar, end_bar)
            VALUES (?, ?, ?, ?, ?)
        """, [file_id, f"seg_{seg_id}", f"segment_{seg_id}", sb, eb])
        seg_id += 1
        bar = eb

def compute_pitch_classes_per_bar(con: duckdb.DuckDBPyConnection, file_id: str) -> None:
    bars_total = con.execute("SELECT 1+MAX(bar) FROM notes WHERE file_id=?", [file_id]).fetchone()[0] or 0
    if bars_total == 0:
        return
    con.execute("""
    CREATE TABLE IF NOT EXISTS pitch_class_spans (
      file_id TEXT, span_id TEXT, start_bar INTEGER, end_bar INTEGER, pcs TEXT
    )""")
    # build bar->pcs
    pcs_by_bar = []
    for b in range(bars_total):
        pitches = [r[0] for r in con.execute(
            "SELECT DISTINCT pitch FROM notes WHERE file_id=? AND bar=?",
            [file_id, b]).fetchall()]
        pcs = sorted({p % 12 for p in pitches})
        pcs_by_bar.append(pcs)
    # compress
    def key(lst): return ",".join(map(str, lst))
    span_id = 0
    i = 0
    while i < bars_total:
        j = i + 1
        while j < bars_total and pcs_by_bar[j] == pcs_by_bar[i]:
            j += 1
        pcs_json = "[" + ",".join(map(str, pcs_by_bar[i])) + "]"
        con.execute("INSERT INTO pitch_class_spans VALUES (?, ?, ?, ?, ?)",
                    [file_id, f"pcspan_{span_id}", i, j, pcs_json])
        span_id += 1
        i = j

def derive_ensemble_objective(con: duckdb.DuckDBPyConnection, file_id: str) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS ensemble (
      file_id TEXT, track_id INTEGER,
      register_low INTEGER, register_high INTEGER,
      enter_bar INTEGER, exit_bar INTEGER
    )""")
    rows = con.execute("""
    WITH per_track AS (
      SELECT track_id,
             MIN(pitch) AS pl, MAX(pitch) AS ph,
             MIN(bar) AS enter_bar, MAX(bar) AS exit_bar
      FROM notes WHERE file_id=? GROUP BY track_id
    )
    SELECT track_id, pl, ph, enter_bar, exit_bar FROM per_track
    """, [file_id]).fetchall()
    for tid, pl, ph, eb, xb in rows:
        con.execute("INSERT INTO ensemble VALUES (?, ?, ?, ?, ?, ?)",
                    [file_id, int(tid), int(pl), int(ph), int(eb), int(xb)])
    con.execute("""
    CREATE TABLE IF NOT EXISTS layering (
      file_id TEXT, event_id TEXT, type TEXT, bar INTEGER, track_id INTEGER
    )""")
    for tid, pl, ph, eb, xb in rows:
        con.execute("INSERT INTO layering VALUES (?, ?, ?, ?, ?)",
                    [file_id, f"enter_{tid}_{eb}", "enter", int(eb), int(tid)])
        con.execute("INSERT INTO layering VALUES (?, ?, ?, ?, ?)",
                    [file_id, f"exit_{tid}_{xb}", "exit", int(xb), int(tid)])

def mine_repeated_pitch_bigrams(con: duckdb.DuckDBPyConnection, file_id: str, min_count: int = 3) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS motifs (
      file_id TEXT, motif_id TEXT, pattern_repr TEXT, n_occ INTEGER
    );""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS motif_occ (
      file_id TEXT, motif_id TEXT, track_id INTEGER, start_bar INTEGER, end_bar INTEGER
    );""")
    track_ids = [r[0] for r in con.execute(
        "SELECT track_id FROM tracks WHERE file_id=?", [file_id]).fetchall()]
    mid = 0
    for tid in track_ids:
        notes = con.execute("""
        SELECT bar, pitch FROM notes WHERE file_id=? AND track_id=? ORDER BY start_s
        """, [file_id, tid]).fetchall()
        if len(notes) < 2:
            continue
        # build bigrams
        from collections import defaultdict
        occ = defaultdict(list)
        for (b1, p1), (b2, p2) in zip(notes, notes[1:]):
            occ[(p1, p2)].append((b1, b2))
        for pattern, spans in occ.items():
            if len(spans) >= min_count:
                patt_str = f"{pattern[0]}->{pattern[1]}"
                motif_id = f"pat_{mid}"; mid += 1
                con.execute("INSERT INTO motifs VALUES (?, ?, ?, ?)",
                            [file_id, motif_id, patt_str, len(spans)])
                for sb, eb in spans:
                    con.execute("INSERT INTO motif_occ VALUES (?, ?, ?, ?, ?)",
                                [file_id, motif_id, tid, int(sb), int(max(sb+1, eb))])


# ----------------------------
# 2.5) Graph creation
# ----------------------------

def create_graph_nodes(con: duckdb.DuckDBPyConnection, file_id: str) -> None:
    """Create graph nodes from symbolic facts."""
    # Create section nodes
    sections = con.execute("""
        SELECT section_id, name, start_bar, end_bar FROM sections WHERE file_id=?
    """, [file_id]).fetchall()
    
    for section_id, name, start_bar, end_bar in sections:
        payload = json.dumps({
            "name": name,
            "start_bar": start_bar,
            "end_bar": end_bar
        })
        node_id = f"sec:{section_id}"
        con.execute("""
            INSERT INTO graph_nodes (file_id, node_id, type, ref_key, payload)
            VALUES (?, ?, ?, ?, ?)
        """, [file_id, node_id, "Section", f"section_id:{section_id}", payload])
    
    # Create track nodes
    tracks = con.execute("""
        SELECT t.track_id, t.program, t.is_drum, t.name, e.register_low, e.register_high, e.enter_bar, e.exit_bar
        FROM tracks t JOIN ensemble e USING (file_id, track_id)
        WHERE t.file_id=?
    """, [file_id]).fetchall()
    
    for track_id, program, is_drum, name, register_low, register_high, enter_bar, exit_bar in tracks:
        payload = json.dumps({
            "program": program,
            "is_drum": is_drum,
            "name": name,
            "register_low": register_low,
            "register_high": register_high,
            "enter_bar": enter_bar,
            "exit_bar": exit_bar
        })
        node_id = f"trk:{track_id}"
        con.execute("""
            INSERT INTO graph_nodes (file_id, node_id, type, ref_key, payload)
            VALUES (?, ?, ?, ?, ?)
        """, [file_id, node_id, "Track", f"track_id:{track_id}", payload])
    
    # Create pitch class span nodes
    pc_spans = con.execute("""
        SELECT span_id, start_bar, end_bar, pcs FROM pitch_class_spans WHERE file_id=?
    """, [file_id]).fetchall()
    
    for span_id, start_bar, end_bar, pcs in pc_spans:
        payload = json.dumps({
            "start_bar": start_bar,
            "end_bar": end_bar,
            "pcs": json.loads(pcs)
        })
        node_id = f"pcs:{span_id}"
        con.execute("""
            INSERT INTO graph_nodes (file_id, node_id, type, ref_key, payload)
            VALUES (?, ?, ?, ?, ?)
        """, [file_id, node_id, "PitchClassSpan", f"span_id:{span_id}", payload])
    
    # Create motif nodes
    motifs = con.execute("""
        SELECT motif_id, pattern_repr, n_occ FROM motifs WHERE file_id=?
    """, [file_id]).fetchall()
    
    for motif_id, pattern_repr, n_occ in motifs:
        payload = json.dumps({
            "pattern_repr": pattern_repr,
            "n_occ": n_occ
        })
        node_id = f"mot:{motif_id}"
        con.execute("""
            INSERT INTO graph_nodes (file_id, node_id, type, ref_key, payload)
            VALUES (?, ?, ?, ?, ?)
        """, [file_id, node_id, "Motif", f"motif_id:{motif_id}", payload])
    
    # Create motif occurrence nodes
    motif_occs = con.execute("""
        SELECT motif_id, track_id, start_bar, end_bar FROM motif_occ WHERE file_id=?
    """, [file_id]).fetchall()
    
    for motif_id, track_id, start_bar, end_bar in motif_occs:
        payload = json.dumps({
            "track_id": track_id,
            "start_bar": start_bar,
            "end_bar": end_bar
        })
        node_id = f"motocc:{motif_id}:{track_id}:{start_bar}"
        con.execute("""
            INSERT INTO graph_nodes (file_id, node_id, type, ref_key, payload)
            VALUES (?, ?, ?, ?, ?)
        """, [file_id, node_id, "MotifOcc", f"motif_id:{motif_id}", payload])

def create_graph_edges(con: duckdb.DuckDBPyConnection, file_id: str) -> None:
    """Create graph edges representing relationships between musical elements."""
    # Create OCCURS_IN edges from tracks to sections
    track_sections = con.execute("""
        SELECT DISTINCT t.track_id, s.section_id, s.start_bar, s.end_bar
        FROM tracks t
        JOIN notes n USING (file_id, track_id)
        JOIN sections s ON s.file_id = t.file_id
        WHERE t.file_id=? AND n.bar >= s.start_bar AND n.bar < s.end_bar
    """, [file_id]).fetchall()
    
    for track_id, section_id, start_bar, end_bar in track_sections:
        src = f"trk:{track_id}"
        dst = f"sec:{section_id}"
        props = json.dumps({"track_id": track_id, "section_id": section_id})
        con.execute("""
            INSERT INTO graph_edges (file_id, src, rel, dst, start_bar, end_bar, props)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [file_id, src, "OCCURS_IN", dst, start_bar, end_bar, props])
    
    # Create PLAYED_BY edges from motif occurrences to tracks
    motif_tracks = con.execute("""
        SELECT motif_id, track_id, start_bar, end_bar FROM motif_occ WHERE file_id=?
    """, [file_id]).fetchall()
    
    for motif_id, track_id, start_bar, end_bar in motif_tracks:
        src = f"motocc:{motif_id}:{track_id}:{start_bar}"
        dst = f"trk:{track_id}"
        props = json.dumps({"motif_id": motif_id, "track_id": track_id})
        con.execute("""
            INSERT INTO graph_edges (file_id, src, rel, dst, start_bar, end_bar, props)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [file_id, src, "PLAYED_BY", dst, start_bar, end_bar, props])
    
    # Create OCCURS_IN edges from motif occurrences to sections
    motif_sections = con.execute("""
        SELECT mo.motif_id, mo.track_id, mo.start_bar, mo.end_bar, s.section_id
        FROM motif_occ mo
        JOIN sections s ON s.file_id = mo.file_id
        WHERE mo.file_id=? AND mo.start_bar >= s.start_bar AND mo.end_bar <= s.end_bar
    """, [file_id]).fetchall()
    
    for motif_id, track_id, start_bar, end_bar, section_id in motif_sections:
        src = f"motocc:{motif_id}:{track_id}:{start_bar}"
        dst = f"sec:{section_id}"
        props = json.dumps({"motif_id": motif_id, "section_id": section_id})
        con.execute("""
            INSERT INTO graph_edges (file_id, src, rel, dst, start_bar, end_bar, props)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [file_id, src, "OCCURS_IN", dst, start_bar, end_bar, props])
    
    # Create DOUBLES edges between tracks that play the same notes in the same bars
    doubles = con.execute("""
        SELECT n1.track_id as track1, n2.track_id as track2, n1.bar, COUNT(*) as note_count
        FROM notes n1
        JOIN notes n2 ON n1.file_id = n2.file_id AND n1.bar = n2.bar AND n1.pitch = n2.pitch
        WHERE n1.file_id=? AND n1.track_id < n2.track_id
        GROUP BY n1.track_id, n2.track_id, n1.bar
        HAVING note_count >= 3  -- At least 3 matching notes to consider as doubling
    """, [file_id]).fetchall()
    
    for track1, track2, bar, note_count in doubles:
        src = f"trk:{track1}"
        dst = f"trk:{track2}"
        props = json.dumps({"track1": track1, "track2": track2, "note_count": note_count})
        con.execute("""
            INSERT INTO graph_edges (file_id, src, rel, dst, start_bar, end_bar, props)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [file_id, src, "DOUBLES", dst, bar, bar+1, props])
    
    # Create SUPPORTS_HARMONY_OF edges from pitch class spans to sections
    harmony_sections = con.execute("""
        SELECT pcs.span_id, s.section_id, pcs.start_bar, pcs.end_bar
        FROM pitch_class_spans pcs
        JOIN sections s ON s.file_id = pcs.file_id
        WHERE pcs.file_id=? AND pcs.start_bar >= s.start_bar AND pcs.end_bar <= s.end_bar
    """, [file_id]).fetchall()
    
    for span_id, section_id, start_bar, end_bar in harmony_sections:
        src = f"pcs:{span_id}"
        dst = f"sec:{section_id}"
        props = json.dumps({"span_id": span_id, "section_id": section_id})
        con.execute("""
            INSERT INTO graph_edges (file_id, src, rel, dst, start_bar, end_bar, props)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [file_id, src, "SUPPORTS_HARMONY_OF", dst, start_bar, end_bar, props])


# ----------------------------
# 3) Facts text (objective-only)
# ----------------------------

def emit_facts_text_objective(con: duckdb.DuckDBPyConnection, file_id: str) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS facts_text (
      file_id TEXT, fact_id TEXT,
      start_bar INTEGER, end_bar INTEGER,
      score_path TEXT, text TEXT
    )""")
    # segments
    for sid, name, sb, eb in con.execute(
        "SELECT section_id, name, start_bar, end_bar FROM sections WHERE file_id=? ORDER BY start_bar",
        [file_id]).fetchall():
        fact = f"Bars {sb}–{eb}: {name}."
        con.execute("INSERT INTO facts_text VALUES (?, ?, ?, ?, ?, ?)",
                    [file_id, str(uuid.uuid4()), sb, eb, f"/segments/{sid}", fact])
    # pitch-class spans
    for sb, eb, pcs_json in con.execute(
        "SELECT start_bar, end_bar, pcs FROM pitch_class_spans WHERE file_id=? ORDER BY start_bar",
        [file_id]).fetchall():
        fact = f"Bars {sb}–{eb}: pitch classes {pcs_json}."
        con.execute("INSERT INTO facts_text VALUES (?, ?, ?, ?, ?, ?)",
                    [file_id, str(uuid.uuid4()), sb, eb, "/pitch_classes", fact])
    # entrances/exits
    for tid, enter_bar, exit_bar in con.execute(
        "SELECT track_id, enter_bar, exit_bar FROM ensemble WHERE file_id=?",
        [file_id]).fetchall():
        con.execute("INSERT INTO facts_text VALUES (?, ?, ?, ?, ?, ?)",
                    [file_id, str(uuid.uuid4()), enter_bar, enter_bar+1,
                     f"/ensemble/{tid}/enter", f"Track {tid} enters at bar {enter_bar}."])
        con.execute("INSERT INTO facts_text VALUES (?, ?, ?, ?, ?, ?)",
                    [file_id, str(uuid.uuid4()), exit_bar, exit_bar+1,
                     f"/ensemble/{tid}/exit", f"Track {tid} exits at bar {exit_bar}."])
    # controllers summary (counts only)
    for ccnum in (1, 7, 10, 11, 64):  # mod, volume, pan, expression, sustain
        cnt = con.execute(
            "SELECT COUNT(*) FROM controllers WHERE file_id=? AND cc=?",
            [file_id, ccnum]).fetchone()[0]
        if cnt and cnt > 0:
            con.execute("INSERT INTO facts_text VALUES (?, ?, ?, ?, ?, ?)",
                        [file_id, str(uuid.uuid4()), 0, 0, f"/controllers/{ccnum}",
                         f"Controller CC{ccnum} present with {int(cnt)} messages."])


# ----------------------------
# 4) ScoreSpec export (objective)
# ----------------------------

def scorespec_from_duckdb(con: duckdb.DuckDBPyConnection, file_id: str) -> dict:
    bpm = con.execute("SELECT ROUND(AVG(bpm),0) FROM tempo_ts WHERE file_id=?", [file_id]).fetchone()[0]
    ts = con.execute("SELECT num,den FROM tsigs WHERE file_id=? ORDER BY time_s LIMIT 1", [file_id]).fetchone()
    meter = f"{ts[0]}/{ts[1]}" if ts else "unknown"

    segs = con.execute("""
      SELECT section_id, start_bar, end_bar FROM sections
      WHERE file_id=? ORDER BY start_bar
    """, [file_id]).fetchall()

    inst = con.execute("""
      SELECT e.track_id, t.program, e.register_low, e.register_high, e.enter_bar, e.exit_bar
      FROM ensemble e JOIN tracks t USING(file_id,track_id)
      WHERE e.file_id=?
    """, [file_id]).fetchall()

    pcs = con.execute("""
      SELECT start_bar, end_bar, pcs FROM pitch_class_spans
      WHERE file_id=? ORDER BY start_bar
    """, [file_id]).fetchall()

    cc = {}
    for ccnum in (1, 7, 10, 11, 64):
        cnt = con.execute("SELECT COUNT(*) FROM controllers WHERE file_id=? AND cc=?",
                          [file_id, ccnum]).fetchone()[0]
        cc[str(ccnum)] = {"present": bool(cnt and cnt > 0), "count": int(cnt or 0)}

    # Get graph nodes and edges
    nodes = con.execute("""
      SELECT node_id, type, ref_key, payload FROM graph_nodes WHERE file_id=?
    """, [file_id]).fetchall()

    edges = con.execute("""
      SELECT src, rel, dst, start_bar, end_bar, props FROM graph_edges WHERE file_id=?
    """, [file_id]).fetchall()

    return {
        "file_id": file_id,
        "global": {"meter": meter, "approx_bpm": int(bpm) if bpm else None},
        "segments": [{"id": sid, "bars": [int(sb), int(eb)]} for sid, sb, eb in segs],
        "instruments": [{"track_id": int(tid), "program": int(prog),
                         "register": {"low": int(rl), "high": int(rh)},
                         "enter_bar": int(eb), "exit_bar": int(xb)}
                        for tid, prog, rl, rh, eb, xb in inst],
        "pitch_class_spans": [{"bars": [int(sb), int(eb)], "pcs": json.loads(pcs_json)}
                              for sb, eb, pcs_json in pcs],
        "controllers": cc,
        "graph": {
            "nodes": [{"node_id": node_id, "type": type, "ref_key": ref_key, "payload": json.loads(payload)}
                      for node_id, type, ref_key, payload in nodes],
            "edges": [{"src": src, "rel": rel, "dst": dst, "start_bar": start_bar, "end_bar": end_bar, "props": json.loads(props)}
                      for src, rel, dst, start_bar, end_bar, props in edges]
        },
        "provenance": {"tables": ["sections", "tracks", "notes", "pitch_class_spans", "controllers", "graph_nodes", "graph_edges"]}
    }


# ----------------------------
# 5) Compose & verify & store
# ----------------------------

def compose_description_objective(con: duckdb.DuckDBPyConnection, file_id: str) -> Tuple[str, List[dict]]:
    bpm = con.execute("SELECT ROUND(AVG(bpm),0) FROM tempo_ts WHERE file_id=?", [file_id]).fetchone()[0]
    ts = con.execute("SELECT num,den FROM tsigs WHERE file_id=? ORDER BY time_s LIMIT 1", [file_id]).fetchone()
    meter = f"{ts[0]}/{ts[1]}" if ts else "unknown"

    parts = [f"Piece in {meter}" + (f", ~{int(bpm)} BPM." if bpm else ".")]

    segs = con.execute("""
        SELECT section_id, name, start_bar, end_bar FROM sections WHERE file_id=? ORDER BY start_bar
    """, [file_id]).fetchall()

    claims = []
    for sid, name, sb, eb in segs:
        progs = con.execute("""
            SELECT DISTINCT t.program
            FROM notes n JOIN tracks t USING(file_id, track_id)
            WHERE n.file_id=? AND n.bar>=? AND n.bar<? ORDER BY t.program
        """, [file_id, sb, eb]).fetchall()
        progs_str = ", ".join(str(p[0]) for p in progs) if progs else "none"

        pcs = con.execute("""
            SELECT pcs FROM pitch_class_spans
            WHERE file_id=? AND NOT (end_bar<=? OR start_bar>=?)
            ORDER BY start_bar LIMIT 1
        """, [file_id, sb, eb]).fetchone()
        pcs_str = pcs[0] if pcs else "[]"

        sent = f"{name} (bars {sb}–{eb}). Instruments (programs): {progs_str}. Representative pitch classes: {pcs_str}."
        parts.append(sent)
        claims.append({"text": sent, "evidence": [f"/segments/{sid}", "/pitch_classes", "/notes"]})

    description = " ".join(parts)
    return description, claims

def verify_description(con: duckdb.DuckDBPyConnection, file_id: str, description: str) -> bool:
    # Simple structural check: each segment sentence exists; bars are plausible.
    max_bar = con.execute("SELECT MAX(bar) FROM notes WHERE file_id=?", [file_id]).fetchone()[0]
    if max_bar is None:
        return False
    segs = con.execute("SELECT name, start_bar, end_bar FROM sections WHERE file_id=? ORDER BY start_bar",
                       [file_id]).fetchall()
    ok = True
    for name, sb, eb in segs:
        if f"{name} (bars {sb}–{eb})" not in description:
            ok = False
    # crude numeric bar plausibility
    import re
    for m in re.finditer(r"bars (\d+)–(\d+)", description):
        a, b = int(m.group(1)), int(m.group(2))
        if a < 0 or b < 0 or a > b or b > max_bar + 1:
            ok = False
    return ok

def store_description(con: duckdb.DuckDBPyConnection, file_id: str, description: str, claims: List[dict]) -> None:
    con.execute("INSERT OR REPLACE INTO descriptions VALUES (?, ?, ?)",
                [file_id, description, json.dumps(claims)])


# ----------------------------
# 6) Build one file (objective mode)
# ----------------------------

def build_objective(con: duckdb.DuckDBPyConnection, midi_path: Path, file_id: Optional[str] = None,
                    export_scorespec_dir: Optional[Path] = None) -> dict:
    file_id = file_id or midi_path.stem
    ingest_midi(con, file_id, midi_path)
    seed_segments(con, file_id, win_bars=8)
    compute_pitch_classes_per_bar(con, file_id)
    derive_ensemble_objective(con, file_id)
    mine_repeated_pitch_bigrams(con, file_id, min_count=3)
    create_graph_nodes(con, file_id)
    create_graph_edges(con, file_id)
    emit_facts_text_objective(con, file_id)

    # Compose (template—you can swap in an LLM later)
    description, claims = compose_description_objective(con, file_id)
    assert verify_description(con, file_id, description), "Verification failed."
    store_description(con, file_id, description, claims)

    # Export ScoreSpec JSON for model I/O
    spec = scorespec_from_duckdb(con, file_id)
    if export_scorespec_dir:
        export_scorespec_dir.mkdir(parents=True, exist_ok=True)
        out = export_scorespec_dir / f"{file_id}.scorespec.json"
        out.write_text(json.dumps(spec, indent=2))
    return spec


# ----------------------------
# 7) CLI
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Objective-mode MIDI -> DuckDB + ScoreSpec builder")
    ap.add_argument("--db", required=True, help="Path to DuckDB file (will be created if missing)")
    ap.add_argument("--in_glob", required=True, help="Glob of MIDI files, e.g., 'data/**/*.mid'")
    ap.add_argument("--export_scorespec_dir", default=None, help="Directory to write per-file ScoreSpec JSON")
    args = ap.parse_args()

    con = connect_db(args.db)
    midi_files = sorted(Path().glob(args.in_glob))
    if not midi_files:
        print("No MIDI files matched your glob.")
        return

    out_dir = Path(args.export_scorespec_dir) if args.export_scorespec_dir else None

    for p in midi_files:
        try:
            spec = build_objective(con, p, file_id=None, export_scorespec_dir=out_dir)
            print(f"✓ Built: {p.name}  →  file_id={spec['file_id']}")
        except Exception as e:
            print(f"✗ Failed: {p}  ({e})")

if __name__ == "__main__":
    main()
