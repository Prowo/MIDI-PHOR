from __future__ import annotations
from typing import Optional
import argparse

import os
import re
import duckdb
import json
import urllib.request

from .slots import build_slots
from .caption import caption_for_song


def build_caption_prompt(con: duckdb.DuckDBPyConnection, song_id: str, section_id: Optional[str] = None, style: str = "short") -> str:
    s = build_slots(con, song_id, section_id)
    mode = "section" if section_id else "song"
    goal = "Write a concise, human-friendly music caption."
    instr = "Use 2 sentences for short; 3-4 for medium. Avoid jargon; describe feel, groove, and harmonic motion."
    delim = "-----"
    return (
f"""{goal}
{instr}

Context: {mode}-level
Meter: {s['meter']}, Tempo: {s['tempo_bpm'] or 'n/a'} BPM
Bars: {s['bars'][0]}-{s['bars'][1]}
Progression: {s['progression']}
Rhythm: {s['rhythm_trait'] or 'n/a'}
Texture: {s['texture_blurb'] or 'n/a'}
Energy (z): {s['energy_z'] if s['energy_z'] is not None else 'n/a'}
Density: {s['density'] if s['density'] is not None else 'n/a'}
Polyphony: {s['polyphony'] if s['polyphony'] is not None else 'n/a'}
Harmonic rhythm: {s['harmonic_rhythm'] if s['harmonic_rhythm'] is not None else 'n/a'}
Tags: {', '.join(s['tags']) if s['tags'] else 'n/a'}
Key events (bar:type/strength): {', '.join(f"{e['bar']}:{e['type']}/{e['strength']:.2f}" for e in s['events']) or 'n/a'}

{delim}
Return only the caption text ({'short' if style=='short' else 'medium'} length).
{delim}
""")


def generate_caption_openai(
    con: duckdb.DuckDBPyConnection,
    song_id: str,
    section_id: Optional[str] = None,
    style: str = "short",
    user_message: Optional[str] = None,
) -> str:
    """
    Request a caption from OpenAI using either a caller-supplied user_message or
    build_caption_prompt(...) when user_message is None.
    Requires OPENAI_API_KEY. Model id from OPENAI_MODEL (default gpt-4o-mini).
    """
    prompt = user_message if user_message is not None else build_caption_prompt(con, song_id, section_id, style)

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


def _anthropic_messages(
    api_key: str,
    base_url: str,
    model: str,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
) -> str:
    url = base_url.rstrip("/") + "/v1/messages"
    payload = {
        "model": model,
        "max_tokens": int(max_tokens),
        "temperature": float(temperature),
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    parts = []
    for c in (obj.get("content") or []):
        if isinstance(c, dict) and c.get("type") == "text" and isinstance(c.get("text"), str):
            parts.append(c["text"])
    return ("\n".join(parts)).strip()



def main() -> int:
    ap = argparse.ArgumentParser(description="Build and/or run the LLM caption prompt")
    ap.add_argument("--db", default="data/musiccap.duckdb", help="Path to DuckDB file")
    ap.add_argument("--song", default=None, help="Song ID to caption; defaults to first in DB")
    ap.add_argument("--section", default=None, help="Optional section_id for section-level prompt")
    ap.add_argument("--style", choices=["short","medium"], default="short", help="Caption length/style")
    ap.add_argument("--backend", choices=["openai", "anthropic"], default=None, help="Backend for --run-caption (defaults to openai if OPENAI_API_KEY set else anthropic)")
    ap.add_argument("--model", default=None, help="Model name (backend-specific). Anthropic default: ANTHROPIC_MODEL or claude-haiku-4-5-20251001")
    ap.add_argument("--dry-run", dest="dry_run", action="store_true", help="Only print the built prompt and exit")
    ap.add_argument("--export-input", action="store_true", help="Write the built caption input prompt to exports/")
    ap.add_argument("--musicgen", action="store_true", help="Compose and save a music-generation prompt using exports + caption")
    ap.add_argument("--compose-caption", action="store_true", help="Compose and save a captioning prompt using exports + DB context")
    ap.add_argument("--out", default=None, help="Optional output path for --musicgen; defaults under exports/")
    ap.add_argument("--no-openai", action="store_true", help="For --musicgen, do not call OpenAI; use template caption instead")
    ap.add_argument("--print-musicgen", action="store_true", help="Also print the composed music-generation prompt to stdout")
    ap.add_argument("--run-musicgen", action="store_true", help="Send the composed music-generation prompt to OpenAI and save output")
    ap.add_argument("--print-caption-prompt", action="store_true", help="Also print the composed captioning prompt to stdout")
    ap.add_argument("--run-caption", action="store_true", help="Send the composed captioning prompt to OpenAI and save caption output")
    ap.add_argument("--caption-out", default=None, help="Optional output path for caption output; defaults under exports/")
    ap.add_argument("--anonymize", action="store_true", help="Remove song names/IDs and file names from composed prompts; strip summary title line")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    # Resolve a song id if not provided
    song_id: Optional[str] = args.song
    if not song_id:
        row = con.execute("SELECT song_id FROM songs ORDER BY song_id LIMIT 1").fetchone()
        if not row:
            print("No songs found in DB")
            return 2
        song_id = row[0]

    prompt = build_caption_prompt(con, song_id, args.section, args.style)
    print(f"SONG_ID: {song_id}")
    print("--- DRY RUN INPUT (Prompt) ---")
    print(prompt)

    # Optionally export the built caption input prompt
    if args.export_input:
        os.makedirs("exports", exist_ok=True)
        safe_song = song_id.replace("/", "_")
        out_path = os.path.join("exports", f"{safe_song}.caption_input.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(prompt)
        print(f"Saved caption input -> {out_path}")

    if args.dry_run:
        # If only dry-run is requested, optionally still build the musicgen prompt
        if not args.musicgen:
            return 0

    # Only generate a seed caption upfront if we need it (musicgen prompt).
    caption_text = None
    if args.musicgen:
        print("--- SEED CAPTION (for musicgen) ---")
        try:
            backend = args.backend
            if backend is None:
                backend = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic"
            if backend == "openai":
                out = generate_caption_openai(con, song_id, args.section, args.style)
            else:
                base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
                model = args.model or os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
                api_key = os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
                if not api_key:
                    raise RuntimeError("Set CLAUDE_API_KEY (or ANTHROPIC_API_KEY) to generate a seed caption with Anthropic")
                out = _anthropic_messages(
                    api_key=api_key,
                    base_url=base_url,
                    model=model,
                    system="You are a concise music captioning assistant.",
                    user=prompt,
                    temperature=0.7,
                    max_tokens=160 if args.style == "short" else 320,
                )
            print(out)
            caption_text = out
        except Exception as e:
            print(f"Seed caption call skipped/failed: {e}")
            try:
                caption_text = caption_for_song(con, song_id)
                print("Using local template caption fallback.")
            except Exception:
                caption_text = None

    # Compose a music-generation prompt that references/inlines exports and caption
    if args.musicgen:
        os.makedirs("exports", exist_ok=True)
        safe_song = song_id.replace("/", "_")
        out_path = args.out or os.path.join("exports", f"{safe_song}.musicgen_prompt.txt")

        # Locate summary and hierarchical-facts exports with tolerant naming
        summary_candidates = [
            os.path.join("exports", f"{song_id}.summary.txt"),
            os.path.join("exports", f"{safe_song}.summary.txt"),
            os.path.join("exports", f"{song_id.replace(' ', '')}.summary.txt"),
        ]
        facts_candidates = [
            os.path.join("exports", f"{song_id}.hierarchical_facts.json"),
            os.path.join("exports", f"{safe_song}.hierarchical_facts.json"),
        ]

        def first_existing(paths: list[str]) -> Optional[str]:
            for p in paths:
                if os.path.exists(p):
                    return p
            return None

        summary_path = first_existing(summary_candidates)
        facts_path = first_existing(facts_candidates)

        summary_txt = None
        facts_txt = None
        if summary_path:
            try:
                with open(summary_path, "r", encoding="utf-8") as f:
                    summary_txt = f.read()
            except Exception:
                pass
        if facts_path:
            try:
                with open(facts_path, "r", encoding="utf-8") as f:
                    facts_txt = f.read()
            except Exception:
                pass

        # Optional anonymization for inlined texts
        def _anonymize_text(text: Optional[str]) -> Optional[str]:
            if text is None:
                return None
            # Drop lines that start with "Title:" (case-insensitive)
            lines = [ln for ln in text.splitlines() if not re.match(r"^\s*Title:\s*", ln, flags=re.IGNORECASE)]
            text = "\n\n".join(lines)
            # Replace occurrences of song identifiers with a neutral phrase
            variants = [song_id, song_id.replace(" ", ""), song_id.replace("/", "_"), song_id.replace(" ", "").replace("/", "_")]
            for v in variants:
                if v:
                    text = re.sub(re.escape(v), "the piece", text, flags=re.IGNORECASE)
            return text

        if args.anonymize:
            summary_txt = _anonymize_text(summary_txt)
            facts_txt = _anonymize_text(facts_txt)

        # If OpenAI is disabled explicitly, try template caption now if not set
        if (args.no_openai or caption_text is None) and caption_text is None:
            try:
                caption_text = caption_for_song(con, song_id)
            except Exception:
                caption_text = None

        delim = "-----"
        parts = []
        parts.append("Goal: Compose a short original music idea inspired by the provided analysis.")
        parts.append("Guidelines: Keep it original; do not copy melodies/harmonies verbatim. Focus on feel, groove, and harmonic motion.")
        parts.append("")
        parts.append(f"{delim}\nCaption (seed idea)\n{delim}")
        parts.append((caption_text or "(no caption available)\n").strip())
        parts.append("")
        if summary_txt:
            header = "Summary (analysis)" if args.anonymize else f"Summary (analysis) — file: @{os.path.basename(summary_path)}"
            parts.append(f"{delim}\n{header}\n{delim}")
            parts.append(summary_txt.strip())
            parts.append("")
        if facts_txt:
            header = "Hierarchical facts" if args.anonymize else f"Hierarchical facts — file: @{os.path.basename(facts_path)}"
            parts.append(f"{delim}\n{header}\n{delim}")
            parts.append(facts_txt.strip())
            parts.append("")
        parts.append(f"{delim}\nOutput format\n{delim}\n- Provide a brief description (2–3 sentences) of the generated idea.\n- Optionally include a compact chord outline (e.g., RN or key+chords).\n- If producing MIDI later, keep tempo and meter explicit.")

        musicgen_prompt = "\n".join(parts) + "\n"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(musicgen_prompt)
        print(f"Saved music-generation prompt -> {out_path}")

        if args.print_musicgen:
            print("=== MUSICGEN PROMPT (BEGIN) ===")
            print(musicgen_prompt)
            print("=== MUSICGEN PROMPT (END) ===")

        if args.run_musicgen:
            try:
                from openai import OpenAI
                api_key = os.environ.get("OPENAI_API_KEY")
                if not api_key:
                    raise RuntimeError("Set OPENAI_API_KEY to run --run-musicgen")
                client = OpenAI(api_key=api_key)
                model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a creative music composition assistant. Produce an original idea description; do not output code."},
                        {"role": "user", "content": musicgen_prompt},
                    ],
                    temperature=0.8,
                    max_tokens=500,
                )
                gen_text = (resp.choices[0].message.content or "").strip()
                gen_out_path = os.path.join("exports", f"{safe_song}.musicgen_output.txt")
                with open(gen_out_path, "w", encoding="utf-8") as f:
                    f.write(gen_text + "\n")
                print(f"Saved music-generation output -> {gen_out_path}")
                print("=== MUSICGEN OUTPUT (BEGIN) ===")
                print(gen_text)
                print("=== MUSICGEN OUTPUT (END) ===")
            except Exception as e:
                print(f"Music-generation call failed/skipped: {e}")

    # Compose a captioning prompt that includes exports as additional context
    if args.compose_caption:
        os.makedirs("exports", exist_ok=True)
        safe_song = song_id.replace("/", "_")
        cap_prompt_path = args.out or os.path.join("exports", f"{safe_song}.caption_prompt.txt")

        # Try to locate exports
        summary_candidates = [
            os.path.join("exports", f"{song_id}.summary.txt"),
            os.path.join("exports", f"{safe_song}.summary.txt"),
            os.path.join("exports", f"{song_id.replace(' ', '')}.summary.txt"),
        ]
        facts_candidates = [
            os.path.join("exports", f"{song_id}.hierarchical_facts.json"),
            os.path.join("exports", f"{safe_song}.hierarchical_facts.json"),
        ]

        def first_existing(paths: list[str]) -> Optional[str]:
            for p in paths:
                if os.path.exists(p):
                    return p
            return None

        summary_path = first_existing(summary_candidates)
        facts_path = first_existing(facts_candidates)

        summary_txt = None
        facts_txt = None
        if summary_path:
            try:
                with open(summary_path, "r", encoding="utf-8") as f:
                    summary_txt = f.read()
            except Exception:
                pass
        if facts_path:
            try:
                with open(facts_path, "r", encoding="utf-8") as f:
                    facts_txt = f.read()
            except Exception:
                pass

        # Optional anonymization for inlined texts
        def _anonymize_text(text: Optional[str]) -> Optional[str]:
            if text is None:
                return None
            lines = [ln for ln in text.splitlines() if not re.match(r"^\s*Title:\s*", ln, flags=re.IGNORECASE)]
            text = "\n\n".join(lines)
            variants = [song_id, song_id.replace(" ", ""), song_id.replace("/", "_"), song_id.replace(" ", "").replace("/", "_")]
            for v in variants:
                if v:
                    text = re.sub(re.escape(v), "the piece", text, flags=re.IGNORECASE)
            return text

        if args.anonymize:
            summary_txt = _anonymize_text(summary_txt)
            facts_txt = _anonymize_text(facts_txt)

        delim = "-----"
        captioning_prompt_parts = []
        captioning_prompt_parts.append("You are a concise music captioning assistant.")
        # Use the DB-derived caption prompt as the single source of instructions to avoid conflicts.
        captioning_prompt_parts.append("")
        if summary_txt:
            header = "Summary (analysis)" if args.anonymize else f"Summary (analysis) — file: @{os.path.basename(summary_path)}"
            captioning_prompt_parts.append(f"{delim}\n{header}\n{delim}")
            captioning_prompt_parts.append(summary_txt.strip())
            captioning_prompt_parts.append("")
        if facts_txt:
            header = "Hierarchical facts" if args.anonymize else f"Hierarchical facts — file: @{os.path.basename(facts_path)}"
            captioning_prompt_parts.append(f"{delim}\n{header}\n{delim}")
            captioning_prompt_parts.append(facts_txt.strip())
            captioning_prompt_parts.append("")
        captioning_prompt_parts.append(f"{delim}\nCaption prompt (from DB)\n{delim}")
        captioning_prompt_parts.append(prompt.strip())
        captioning_prompt_parts.append("")
        # Add an explicit, non-conflicting output format section (mirrors musicgen style)
        captioning_prompt_parts.append(f"{delim}\nOutput format\n{delim}\n- Return only the caption text.\n- Length: {'3–4 sentences (medium)' if args.style=='medium' else '2 sentences (short)'}.\n- Avoid jargon; focus on feel, groove, and harmonic motion.")

        captioning_prompt = "\n".join(captioning_prompt_parts) + "\n"
        with open(cap_prompt_path, "w", encoding="utf-8") as f:
            f.write(captioning_prompt)
        print(f"Saved captioning prompt -> {cap_prompt_path}")

        if args.print_caption_prompt:
            print("=== CAPTION PROMPT (BEGIN) ===")
            print(captioning_prompt)
            print("=== CAPTION PROMPT (END) ===")

        if args.run_caption:
            try:
                backend = args.backend
                if backend is None:
                    backend = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic"

                if backend == "openai":
                    from openai import OpenAI

                    api_key = os.environ.get("OPENAI_API_KEY")
                    if not api_key:
                        raise RuntimeError("Set OPENAI_API_KEY to run --run-caption (openai backend)")
                    client = OpenAI(api_key=api_key)
                    model = args.model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
                    resp = client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": "You are a concise music captioning assistant."},
                            {"role": "user", "content": captioning_prompt},
                        ],
                        temperature=0.7,
                        max_tokens=320 if args.style == "medium" else 160,
                    )
                    cap_text = (resp.choices[0].message.content or "").strip()
                else:
                    api_key = os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
                    if not api_key:
                        raise RuntimeError("Set CLAUDE_API_KEY (or ANTHROPIC_API_KEY) to run --run-caption (anthropic backend)")
                    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
                    model = args.model or os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
                    cap_text = _anthropic_messages(
                        api_key=api_key,
                        base_url=base_url,
                        model=model,
                        system="You are a concise music captioning assistant.",
                        user=captioning_prompt,
                        temperature=0.7,
                        max_tokens=320 if args.style == "medium" else 160,
                    )
                cap_out_path = args.caption_out or os.path.join("exports", f"{safe_song}.caption_output.txt")
                with open(cap_out_path, "w", encoding="utf-8") as f:
                    f.write(cap_text + "\n")
                print(f"Saved caption output -> {cap_out_path}")
                print("=== CAPTION OUTPUT (BEGIN) ===")
                print(cap_text)
                print("=== CAPTION OUTPUT (END) ===")
            except Exception as e:
                print(f"Captioning call failed/skipped: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
