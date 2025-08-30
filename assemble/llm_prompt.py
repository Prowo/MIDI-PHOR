from __future__ import annotations
from typing import Optional

import os
import duckdb

from .slots import build_slots


def build_caption_prompt(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str] = None, style: str = "short") -> str:
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
Bars: {s['bars'][0]}–{s['bars'][1]}
Progression: {s['progression']}
Rhythm: {s['rhythm_trait'] or '—'}
Texture: {s['texture_blurb'] or '—'}
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


def generate_caption_openai(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str] = None, style: str = "short") -> str:
    """
    Build a prompt from DB features and request a caption from OpenAI GPT-4o-mini.
    Requires environment variable OPENAI_API_KEY to be set.
    """
    prompt = build_caption_prompt(con, song_id, section_id, style)

    # Lazy import to avoid hard dependency if not used
    try:
        from openai import OpenAI
    except Exception as e:
        raise RuntimeError("openai package not installed: pip install openai") from e

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENAI_API_KEY environment variable to call OpenAI API")

    client = OpenAI(api_key=api_key)
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    # Use chat completions for best formatting control
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a concise music captioning assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.7,
        max_tokens=160 if style == "short" else 320,
    )

    text = (resp.choices[0].message.content or "").strip()
    return text


