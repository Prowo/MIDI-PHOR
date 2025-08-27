# assemble/prompt.py
from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple
import json, duckdb

def _q(con, sql, *args):
    return con.execute(sql, list(args)).fetchall()

def _first(con, sql, *args):
    r = con.execute(sql, list(args)).fetchone()
    return r[0] if r and r[0] is not None else None

def _meter_and_tempo(con, song_id):
    ts = _q(con, "SELECT num,den FROM timesig_changes WHERE song_id=? ORDER BY t_sec LIMIT 1", song_id)
    meter = f"{ts[0][0]}/{ts[0][1]}" if ts else "unknown"
    bpm = _first(con, "SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?", song_id)
    return meter, (int(bpm) if bpm is not None else None)

def _roman_seq(con, song_id, sb, eb, max_len=10):
    rows = _q(con, """
        SELECT rn FROM chords
        WHERE song_id=? AND onset_bar BETWEEN ? AND ?
        ORDER BY onset_bar, onset_beat LIMIT ?
    """, song_id, int(sb), int(eb), max_len)
    if not rows: return None
    seq = []
    for (rn,) in rows:
        if not seq or seq[-1] != rn: seq.append(rn)
    return "–".join(seq)

def _avgf(con, song_id, sb, eb, feat):
    return _first(con, """
        SELECT AVG(value) FROM ts_bar
        WHERE song_id=? AND feature=? AND bar BETWEEN ? AND ?
    """, song_id, feat, int(sb), int(eb))

def _top_tags(con, song_id, section_id=None):
    if section_id:
        rows = _q(con, """
            SELECT tag FROM tags_section
            WHERE song_id=? AND section_id=? AND tag_type IN ('genre','mood','timbre')
            ORDER BY confidence DESC LIMIT 3
        """, song_id, section_id)
    else:
        rows = _q(con, """
            SELECT tag FROM tags_section
            WHERE song_id=? AND section_id='S_global'
            ORDER BY confidence DESC LIMIT 3
        """, song_id)
    return [r[0] for r in rows]

def _texture(con, song_id, sb, eb):
    rows = _q(con, """
        SELECT t.name, COUNT(*) c
        FROM notes n JOIN tracks t USING(song_id,track_id)
        WHERE n.song_id=? AND n.onset_bar BETWEEN ? AND ?
        GROUP BY t.name ORDER BY c DESC LIMIT 3
    """, song_id, int(sb), int(eb))
    names = [nm for (nm, _) in rows if nm]
    if not names: return None
    if len(names)==1: return f"{names[0]} leads"
    if len(names)==2: return f"{names[0]} with {names[1]}"
    return f"{names[0]}, {names[1]} with {names[2]} accents"

def _salient_events(con, song_id, sb, eb, top_n=5):
    rows = _q(con, """
        SELECT bar, event_type, COALESCE(strength,0.0) s
        FROM events
        WHERE song_id=? AND bar BETWEEN ? AND ?
        ORDER BY 
          CASE event_type
            WHEN 'CLIMAX' THEN 0
            WHEN 'DROP' THEN 1
            WHEN 'SECTION_BOUNDARY' THEN 2
            WHEN 'ENTRY_DRUMS' THEN 3
            ELSE 4
          END, s DESC, bar
        LIMIT ?
    """, song_id, int(sb), int(eb), int(top_n))
    return [{"bar": int(b), "type": et, "strength": float(s)} for (b, et, s) in rows]

def build_slots(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str]=None) -> Dict[str, Any]:
    if section_id:
        r = _q(con, "SELECT start_bar,end_bar FROM sections WHERE song_id=? AND section_id=?", song_id, section_id)
        if not r: raise ValueError(f"Unknown section_id {section_id}")
        sb, eb = map(int, r[0])
    else:
        r = _q(con, "SELECT MIN(bar), MAX(bar) FROM bars WHERE song_id=?", song_id)
        sb, eb = int(r[0][0] or 1), int(r[0][1] or 1)

    meter, bpm = _meter_and_tempo(con, song_id)
    energy = _avgf(con, song_id, sb, eb, "energy_bar_z")
    density = _avgf(con, song_id, sb, eb, "density")
    poly    = _avgf(con, song_id, sb, eb, "polyphony")
    back    = _avgf(con, song_id, sb, eb, "backbeat_strength")
    sync    = _avgf(con, song_id, sb, eb, "syncopation")
    harmrh  = _avgf(con, song_id, sb, eb, "harmonic_rhythm")
    roman   = _roman_seq(con, song_id, sb, eb)
    texture = _texture(con, song_id, sb, eb)
    tags    = _top_tags(con, song_id, section_id)

    rhythm_trait = None
    if back is not None and back >= 0.25: rhythm_trait = "strong backbeat"
    elif sync is not None and sync >= 0.4: rhythm_trait = "syncopated off-beats"

    return {
        "song_id": song_id,
        "span_bars": [sb, eb],
        "meter": meter,
        "tempo_bpm": bpm,
        "progression": roman or "simple diatonic moves",
        "rhythm_trait": rhythm_trait,
        "energy_z": energy,
        "density": density,
        "polyphony": poly,
        "harmonic_rhythm": harmrh,
        "texture": texture,
        "tags": tags,
        "events": _salient_events(con, song_id, sb, eb, top_n=6),
        "section_id": section_id
    }

def build_caption_prompt(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str]=None, style: str="short") -> str:
    """
    Returns a compact text prompt you can send to an LLM.
    """
    s = build_slots(con, song_id, section_id)
    mode = "section" if section_id else "song"
    goal = "Write a concise, human-friendly music caption."
    instr = "Use 2 sentences for short; 3–4 for medium. Avoid jargon; describe feel, groove, and harmonic motion."
    delim = "-----"
    return (
f"""{goal}
{instr}

Context: {mode}-level
Meter: {s['meter']}, Tempo: {s['tempo_bpm'] or '—'} BPM
Bars: {s['span_bars'][0]}–{s['span_bars'][1]}
Progression: {s['progression']}
Rhythm: {s['rhythm_trait'] or '—'}
Texture: {s['texture'] or '—'}
Energy (z): {s['energy_z'] if s['energy_z'] is not None else 'n/a'}
Density: {s['density'] if s['density'] is not None else 'n/a'}
Polyphony: {s['polyphony'] if s['polyphony'] is not None else 'n/a'}
Harmonic rhythm: {s['harmonic_rhythm'] if s['harmonic_rhythm'] is not None else 'n/a'}
Tags: {', '.join(s['tags']) if s['tags'] else '—'}
Key events (bar:type/strength): {', '.join(f"{e['bar']}:{e['type']}/{e['strength']:.2f}" for e in s['events']) or '—'}

{delim}
Return only the caption text ({'short' if style=='short' else 'medium'} length).
{delim}
""")

