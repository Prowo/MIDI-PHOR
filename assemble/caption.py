# assemble/captioner.py
from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple
try:
    # music21 is used to convert Roman numerals to absolute chord names
    from music21 import key as m21key, roman as m21roman
except Exception:
    m21key = None
    m21roman = None
import duckdb
from .slots import build_slots

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

def _roman_seq_with_names(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int,int], max_len: int = 8) -> Optional[str]:
    roman = _roman_seq(con, song_id, bars, max_len=max_len)
    if not roman:
        return None
    # If music21 is unavailable, return RN only
    if m21key is None or m21roman is None:
        return roman
    key_str = _song_key(con, song_id, bars)
    if not key_str:
        return roman
    try:
        root, mode = key_str.split(":")
        mode = "major" if mode.lower().startswith("maj") else ("minor" if mode.lower().startswith("min") else mode)
        k = m21key.Key(root, mode)
        names: List[str] = []
        for tok in roman.split("–"):
            try:
                rn = m21roman.RomanNumeral(tok, k)
                ch = rn.toChord()
                root_name = ch.root().name if ch.root() is not None else rn.root().name
                # crude symbol mapping for readability
                qual = (ch.commonName or "").lower()
                sym = ""
                if "minor" in qual:
                    sym = "m"
                elif "diminished" in qual:
                    sym = "dim"
                elif "augmented" in qual:
                    sym = "aug"
                # naive seventh detection from RN token
                if "7" in tok and "major" not in qual and "diminished" not in qual and sym not in ("dim","aug"):
                    sym = sym + "7"
                names.append(root_name + sym)
            except Exception:
                names.append(tok)
        return f"{roman} (" + "–".join(names) + ")"
    except Exception:
        return roman

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

# DEPRECATED: use build_slots in assemble/slots.py
def slots_for_section(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str] = None) -> Dict[str, Any]:
    return build_slots(con, song_id, section_id)

# ---------- Caption templates ----------

def caption_short(slots: Dict[str, Any]) -> str:
    bits = []
    if slots.get("tempo_bpm"): bits.append(f"~{int(slots['tempo_bpm'])} BPM")
    if slots.get("meter"): bits.append(slots["meter"])
    head = ", ".join(bits) if bits else ""
    key = slots.get("key") or "unknown key"
    prog = slots.get("progression") or "simple diatonic moves"
    s1 = f"{head} piece in {key} built around {prog}."

    s2_parts = []
    if slots.get("texture_blurb"): s2_parts.append(slots["texture_blurb"])
    if slots.get("rhythm_trait"): s2_parts.append(slots["rhythm_trait"])
    # prefer mood then genre
    mood = None
    genre = None
    for t in slots.get("tags", []):
        if t in ("energetic","mellow","dreamy","aggressive","uplifting","dark","bright"):
            mood = mood or t
        if t and isinstance(t, str) and t.lower() in ("pop","rock","jazz","edm","hip hop","ambient","classical","metal","funk","house","techno"):
            genre = genre or t
    mood_or_genre = mood or genre
    if mood_or_genre: s2_parts.append(mood_or_genre)

    s2 = (" ".join([p + "." for p in s2_parts])).replace("..", ".")
    return (s1 + " " + s2).strip()

def caption_medium(slots: Dict[str, Any]) -> str:
    bars = slots["bars"]
    intro = f"{slots.get('tempo_bpm') or '—'} BPM, {slots.get('meter','—')}."
    key = slots.get("key") or "unknown key"
    s = [f"{intro} In {key}."]

    prog = slots.get("progression") or "simple diatonic moves"
    rhythm = slots.get("rhythm_trait")
    texture = slots.get("texture_blurb")
    mood = None
    genre = None
    for t in slots.get("tags", []):
        if t in ("energetic","mellow","dreamy","aggressive","uplifting","dark","bright"):
            mood = mood or t
        if t and isinstance(t, str) and t.lower() in ("pop","rock","jazz","edm","hip hop","ambient","classical","metal","funk","house","techno"):
            genre = genre or t
    mood = mood or genre

    body = []
    body.append(f"Bars {bars[0]}–{bars[1]} feature {prog}.")
    if texture: body.append(texture + ".")
    if rhythm: body.append(rhythm + ".")
    if mood: body.append(f"Overall feel: {mood}.")
    s.append(" ".join(body))
    return " ".join(s)

# ---------- One-liners ----------

def caption_for_song(con: duckdb.DuckDBPyConnection, song_id: str) -> str:
    slots = build_slots(con, song_id, section_id=None)
    return caption_short(slots)

def captions_by_section(con: duckdb.DuckDBPyConnection, song_id: str, mode: str = "short") -> List[Tuple[str,str]]:
    rows = con.execute("""
        SELECT section_id FROM sections WHERE song_id=? AND source='merged' ORDER BY start_bar
    """, [song_id]).fetchall()
    out = []
    for (sid,) in rows:
        slots = build_slots(con, song_id, section_id=sid)
        text = caption_short(slots) if mode == "short" else caption_medium(slots)
        out.append((sid, text))
    return out
