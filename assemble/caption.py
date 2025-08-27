# assemble/captioner.py
from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple
import duckdb

# ---------- Slot assembly ----------

def _song_key(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int,int]) -> Optional[str]:
    """
    Fetch a symbolic key label from key_changes.
    Strategy: prefer the earliest key change within the span; otherwise earliest in song.
    """
    row = con.execute(
        """
        SELECT key FROM key_changes
        WHERE song_id=? AND at_bar BETWEEN ? AND ?
        ORDER BY at_bar, at_beat LIMIT 1
        """,
        [song_id, int(bars[0]), int(bars[1])]
    ).fetchone()
    if row and row[0]:
        return str(row[0])
    row = con.execute(
        """
        SELECT key FROM key_changes
        WHERE song_id=?
        ORDER BY at_bar, at_beat LIMIT 1
        """,
        [song_id]
    ).fetchone()
    return (str(row[0]) if row and row[0] else None)

def _avg_bar_feature(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int,int], feature: str) -> Optional[float]:
    row = con.execute("""
        SELECT AVG(value) FROM ts_bar
        WHERE song_id=? AND feature=? AND bar BETWEEN ? AND ?
    """, [song_id, feature, int(bars[0]), int(bars[1])]).fetchone()
    return float(row[0]) if row and row[0] is not None else None

def _top_tags(con: duckdb.DuckDBPyConnection, song_id: str, section_id: str) -> List[str]:
    rows = con.execute("""
        SELECT tag FROM tags_section
        WHERE song_id=? AND section_id=? AND tag_type IN ('genre','mood','timbre')
        ORDER BY confidence DESC LIMIT 3
    """, [song_id, section_id]).fetchall()
    return [r[0] for r in rows]

def _roman_seq(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int,int], max_len: int = 8) -> Optional[str]:
    rows = con.execute("""
        SELECT rn FROM chords
        WHERE song_id=? AND onset_bar BETWEEN ? AND ?
        ORDER BY onset_bar, onset_beat
        LIMIT ?
    """, [song_id, int(bars[0]), int(bars[1]), max_len]).fetchall()
    if not rows:
        return None
    # compress adjacent duplicates
    seq = []
    for (rn,) in rows:
        if not seq or seq[-1] != rn:
            seq.append(rn)
    return "–".join(seq)

def _meter_and_tempo(con: duckdb.DuckDBPyConnection, song_id: str) -> Tuple[str, Optional[int]]:
    ts = con.execute("""
        SELECT num, den FROM timesig_changes
        WHERE song_id=? ORDER BY t_sec LIMIT 1
    """, [song_id]).fetchone()
    meter = f"{ts[0]}/{ts[1]}" if ts else "unknown"
    bpm = con.execute("""
        SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?
    """, [song_id]).fetchone()[0]
    return meter, (int(bpm) if bpm is not None else None)

def _texture_blurb(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int,int]) -> Optional[str]:
    # simple: which tracks are active most in this span?
    rows = con.execute("""
        SELECT t.name, COUNT(*) AS c
        FROM notes n JOIN tracks t USING(song_id, track_id)
        WHERE n.song_id=? AND n.onset_bar BETWEEN ? AND ?
        GROUP BY t.name
        ORDER BY c DESC
        LIMIT 3
    """, [song_id, int(bars[0]), int(bars[1])]).fetchall()
    names = [nm for (nm, _) in rows if nm]
    if not names:
        return None
    if len(names) == 1:
        return f"{names[0]} leads"
    if len(names) == 2:
        return f"{names[0]} with {names[1]}"
    return f"{names[0]}, {names[1]} with {names[2]} accents"

def slots_for_section(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Assemble caption slots for a section (or whole song if section_id=None).
    Returns a dict consumable by caption_short/medium.
    """
    # pick span
    if section_id:
        row = con.execute("""
            SELECT start_bar, end_bar FROM sections
            WHERE song_id=? AND section_id=?
        """, [song_id, section_id]).fetchone()
        if not row:
            raise ValueError(f"Unknown section_id {section_id} for {song_id}")
        bars = (int(row[0]), int(row[1]))
    else:
        # entire song span
        row = con.execute("SELECT MIN(bar), MAX(bar) FROM bars WHERE song_id=?", [song_id]).fetchone()
        bars = (int(row[0] or 1), int(row[1] or 1))

    meter, tempo = _meter_and_tempo(con, song_id)
    # features: prefer audio energy if present, else fall back to density as proxy
    energy = _avg_bar_feature(con, song_id, bars, "energy_bar_z")
    density = _avg_bar_feature(con, song_id, bars, "density")
    poly    = _avg_bar_feature(con, song_id, bars, "polyphony")
    backbeat= _avg_bar_feature(con, song_id, bars, "backbeat_strength")
    syncop  = _avg_bar_feature(con, song_id, bars, "syncopation")
    harmrh  = _avg_bar_feature(con, song_id, bars, "harmonic_rhythm")
    roman   = _roman_seq(con, song_id, bars, max_len=8)
    texture = _texture_blurb(con, song_id, bars)

    tags = []
    if section_id:
        tags = _top_tags(con, song_id, section_id)

    # derive rhythm trait
    rhythm_trait = None
    if backbeat is not None and backbeat >= 0.25:
        rhythm_trait = "strong backbeat"
    elif syncop is not None and syncop >= 0.4:
        rhythm_trait = "syncopated off-beats"

    # derive progression label
    progression_label = roman or "simple diatonic moves"

    # feel/mood from tags if present
    mood = None
    genre = None
    for t in tags:
        if t in ("energetic","mellow","dreamy","aggressive","uplifting","dark","bright"):
            mood = mood or t
        # naive genre bucket
        if t.lower() in ("pop","rock","jazz","edm","hip hop","ambient","classical","metal","funk","house","techno"):
            genre = genre or t

    return {
        "bars": bars,
        "meter": meter,
        "tempo_bpm": tempo,
        "key": _song_key(con, song_id, bars),
        "progression_label": progression_label,
        "rhythm_trait": rhythm_trait,
        "density": density,
        "polyphony": poly,
        "energy_z": energy,
        "texture_blurb": texture,
        "mood": mood,
        "genre": genre,
        "tags": tags,
        "section_id": section_id,
    }

# ---------- Caption templates ----------

def caption_short(slots: Dict[str, Any]) -> str:
    bits = []
    if slots.get("tempo_bpm"): bits.append(f"~{int(slots['tempo_bpm'])} BPM")
    if slots.get("meter"): bits.append(slots["meter"])
    head = ", ".join(bits) if bits else ""
    key = slots.get("key") or "unknown key"
    prog = slots.get("progression_label") or "simple diatonic moves"
    s1 = f"{head} piece in {key} built around {prog}."

    s2_parts = []
    if slots.get("texture_blurb"): s2_parts.append(slots["texture_blurb"])
    if slots.get("rhythm_trait"): s2_parts.append(slots["rhythm_trait"])
    # prefer mood then genre
    mood_or_genre = slots.get("mood") or slots.get("genre")
    if mood_or_genre: s2_parts.append(mood_or_genre)

    s2 = (" ".join([p + "." for p in s2_parts])).replace("..", ".")
    return (s1 + " " + s2).strip()

def caption_medium(slots: Dict[str, Any]) -> str:
    bars = slots["bars"]
    intro = f"{slots.get('tempo_bpm') or '—'} BPM, {slots.get('meter','—')}."
    key = slots.get("key") or "unknown key"
    s = [f"{intro} In {key}."]

    prog = slots.get("progression_label") or "simple diatonic moves"
    rhythm = slots.get("rhythm_trait")
    texture = slots.get("texture_blurb")
    mood = slots.get("mood") or slots.get("genre")

    body = []
    body.append(f"Bars {bars[0]}–{bars[1]} feature {prog}.")
    if texture: body.append(texture + ".")
    if rhythm: body.append(rhythm + ".")
    if mood: body.append(f"Overall feel: {mood}.")
    s.append(" ".join(body))
    return " ".join(s)

# ---------- One-liners ----------

def caption_for_song(con: duckdb.DuckDBPyConnection, song_id: str) -> str:
    slots = slots_for_section(con, song_id, section_id=None)
    return caption_short(slots)

def captions_by_section(con: duckdb.DuckDBPyConnection, song_id: str, mode: str = "short") -> List[Tuple[str,str]]:
    rows = con.execute("""
        SELECT section_id FROM sections WHERE song_id=? AND source='merged' ORDER BY start_bar
    """, [song_id]).fetchall()
    out = []
    for (sid,) in rows:
        slots = slots_for_section(con, song_id, section_id=sid)
        text = caption_short(slots) if mode == "short" else caption_medium(slots)
        out.append((sid, text))
    return out
