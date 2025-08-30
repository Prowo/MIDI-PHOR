from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple

try:
    # Optional: used to render Roman numerals to absolute chord names
    from music21 import key as m21key, roman as m21roman
except Exception:
    m21key = None
    m21roman = None

import duckdb


# ---------- Core DB helpers ----------

def get_span(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str]) -> Tuple[int, int]:
    """
    Return (start_bar, end_bar) for a section; or whole-song span if section_id is None.
    """
    if section_id:
        row = con.execute(
            """
            SELECT start_bar, end_bar FROM sections
            WHERE song_id=? AND section_id=?
            """,
            [song_id, section_id],
        ).fetchone()
        if not row:
            raise ValueError(f"Unknown section_id {section_id} for {song_id}")
        return int(row[0]), int(row[1])
    row = con.execute(
        "SELECT MIN(bar), MAX(bar) FROM bars WHERE song_id=?",
        [song_id],
    ).fetchone()
    return int(row[0] or 1), int(row[1] or 1)


def meter_and_tempo(con: duckdb.DuckDBPyConnection, song_id: str) -> Tuple[str, Optional[int]]:
    ts = con.execute(
        """
        SELECT num, den FROM timesig_changes
        WHERE song_id=? ORDER BY t_sec LIMIT 1
        """,
        [song_id],
    ).fetchone()
    meter = f"{ts[0]}/{ts[1]}" if ts else "unknown"
    bpm = con.execute(
        "SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?",
        [song_id],
    ).fetchone()[0]
    return meter, (int(bpm) if bpm is not None else None)


def avg_bar_feature(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int, int], feature: str) -> Optional[float]:
    row = con.execute(
        """
        SELECT AVG(value) FROM ts_bar
        WHERE song_id=? AND feature=? AND bar BETWEEN ? AND ?
        """,
        [song_id, feature, int(bars[0]), int(bars[1])],
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def song_key(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int, int]) -> Optional[str]:
    """
    Fetch a symbolic key label from key_changes. Prefer earliest within span, else earliest overall.
    """
    row = con.execute(
        """
        SELECT key FROM key_changes
        WHERE song_id=? AND at_bar BETWEEN ? AND ?
        ORDER BY at_bar, at_beat LIMIT 1
        """,
        [song_id, int(bars[0]), int(bars[1])],
    ).fetchone()
    if row and row[0]:
        return str(row[0])
    row = con.execute(
        """
        SELECT key FROM key_changes
        WHERE song_id=?
        ORDER BY at_bar, at_beat LIMIT 1
        """,
        [song_id],
    ).fetchone()
    return (str(row[0]) if row and row[0] else None)


def roman_seq(
    con: duckdb.DuckDBPyConnection,
    song_id: str,
    bars: Tuple[int, int],
    max_len: int = 8,
    with_names: bool = False,
) -> Optional[str]:
    rows = con.execute(
        """
        SELECT rn FROM chords
        WHERE song_id=? AND onset_bar BETWEEN ? AND ?
        ORDER BY onset_bar, onset_beat
        LIMIT ?
        """,
        [song_id, int(bars[0]), int(bars[1]), int(max_len)],
    ).fetchall()
    if not rows:
        return None
    seq: List[str] = []
    for (rn,) in rows:
        if not seq or seq[-1] != rn:
            seq.append(rn)
    roman = "–".join(seq)
    if not with_names:
        return roman

    # Convert to chord names if music21 is available and a key can be inferred
    if m21key is None or m21roman is None:
        return roman
    k = song_key(con, song_id, bars)
    if not k:
        return roman
    try:
        root, mode = k.split(":")
        mode = "major" if mode.lower().startswith("maj") else ("minor" if mode.lower().startswith("min") else mode)
        key_obj = m21key.Key(root, mode)
        names: List[str] = []
        for tok in roman.split("–"):
            try:
                rn = m21roman.RomanNumeral(tok, key_obj)
                ch = rn.toChord()
                root_name = ch.root().name if ch.root() is not None else rn.root().name
                qual = (ch.commonName or "").lower()
                sym = ""
                if "minor" in qual:
                    sym = "m"
                elif "diminished" in qual:
                    sym = "dim"
                elif "augmented" in qual:
                    sym = "aug"
                if "7" in tok and "major" not in qual and "diminished" not in qual and sym not in ("dim","aug"):
                    sym = sym + "7"
                names.append(root_name + sym)
            except Exception:
                names.append(tok)
        return f"{roman} (" + "–".join(names) + ")"
    except Exception:
        return roman


def top_tags(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str]) -> List[str]:
    if section_id:
        rows = con.execute(
            """
            SELECT tag FROM tags_section
            WHERE song_id=? AND section_id=? AND tag_type IN ('genre','mood','timbre')
            ORDER BY confidence DESC LIMIT 3
            """,
            [song_id, section_id],
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT tag FROM tags_section
            WHERE song_id=? AND section_id='S_global'
            ORDER BY confidence DESC LIMIT 3
            """,
            [song_id],
        ).fetchall()
    return [r[0] for r in rows]


def texture_blurb(con: duckdb.DuckDBPyConnection, song_id: str, bars: Tuple[int, int]) -> Optional[str]:
    rows = con.execute(
        """
        SELECT t.name, COUNT(*) AS c
        FROM notes n JOIN tracks t USING(song_id, track_id)
        WHERE n.song_id=? AND n.onset_bar BETWEEN ? AND ?
        GROUP BY t.name
        ORDER BY c DESC
        LIMIT 3
        """,
        [song_id, int(bars[0]), int(bars[1])],
    ).fetchall()
    names = [nm for (nm, _) in rows if nm]
    if not names:
        return None
    if len(names) == 1:
        return f"{names[0]} leads"
    if len(names) == 2:
        return f"{names[0]} with {names[1]}"
    return f"{names[0]}, {names[1]} with {names[2]} accents"


def salient_events(
    con: duckdb.DuckDBPyConnection,
    song_id: str,
    bars: Tuple[int, int],
    top_n: int = 6,
) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
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
        """,
        [song_id, int(bars[0]), int(bars[1]), int(top_n)],
    ).fetchall()
    return [{"bar": int(b), "type": et, "strength": float(s)} for (b, et, s) in rows]


# ---------- Canonical slots ----------

def build_slots(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Build a canonical slots dict used by captioning and prompt builders.
    Keys:
      - bars: (start_bar, end_bar)
      - meter: str, tempo_bpm: Optional[int]
      - key: Optional[str]
      - roman: Optional[str] (RN sequence)
      - progression: str (RN sequence, optionally with names)
      - rhythm_trait: Optional[str]
      - energy_z, density, polyphony, backbeat_strength, syncopation, harmonic_rhythm: Optional[float]
      - texture_blurb: Optional[str]
      - tags: List[str]
      - events: List[Dict]
      - section_id: Optional[str]
    """
    bars = get_span(con, song_id, section_id)
    meter, tempo = meter_and_tempo(con, song_id)

    energy = avg_bar_feature(con, song_id, bars, "energy_bar_z")
    density = avg_bar_feature(con, song_id, bars, "density")
    poly = avg_bar_feature(con, song_id, bars, "polyphony")
    back = avg_bar_feature(con, song_id, bars, "backbeat_strength")
    sync = avg_bar_feature(con, song_id, bars, "syncopation")
    harmrh = avg_bar_feature(con, song_id, bars, "harmonic_rhythm")

    rn_only = roman_seq(con, song_id, bars, max_len=8, with_names=False)
    rn_named = roman_seq(con, song_id, bars, max_len=8, with_names=True)

    texture = texture_blurb(con, song_id, bars)
    tags = top_tags(con, song_id, section_id)

    rhythm_trait: Optional[str] = None
    if back is not None and back >= 0.25:
        rhythm_trait = "strong backbeat"
    elif sync is not None and sync >= 0.4:
        rhythm_trait = "syncopated off-beats"

    ev = salient_events(con, song_id, bars, top_n=6)

    return {
        "bars": bars,
        "meter": meter,
        "tempo_bpm": tempo,
        "key": song_key(con, song_id, bars),
        "roman": rn_only,
        "progression": rn_named or rn_only or "simple diatonic moves",
        "rhythm_trait": rhythm_trait,
        "energy_z": energy,
        "density": density,
        "polyphony": poly,
        "backbeat_strength": back,
        "syncopation": sync,
        "harmonic_rhythm": harmrh,
        "texture_blurb": texture,
        "tags": tags,
        "events": ev,
        "section_id": section_id,
    }


