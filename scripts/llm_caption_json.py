#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.duck import connect, ensure_schema
from assemble.slots import build_slots
from assemble.caption import caption_short


SCHEMA_VERSION = "midiphor.claims.v1"


def _read_windows_env_registry(name: str) -> Optional[str]:
    """
    Read a user/machine environment variable from the Windows registry.
    Useful when variables were set with `setx` but the current process wasn't restarted.
    """
    if platform.system().lower() != "windows":
        return None
    try:
        import winreg  # type: ignore

        for hive, subkey in [
            (winreg.HKEY_CURRENT_USER, r"Environment"),
            (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
        ]:
            try:
                with winreg.OpenKey(hive, subkey) as k:
                    val, _ = winreg.QueryValueEx(k, name)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
            except Exception:
                continue
    except Exception:
        return None
    return None


def _json_extract(text: str) -> Dict[str, Any]:
    """
    Extract a JSON object from LLM output. Accepts either raw JSON or text containing a JSON block.
    """
    s = text.strip()
    if s.startswith("{") and s.endswith("}"):
        return json.loads(s)

    # Robust extraction: try to parse the first valid JSON object beginning at any '{'.
    dec = json.JSONDecoder()
    for m in re.finditer(r"\{", s):
        try:
            obj, _end = dec.raw_decode(s[m.start() :])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue

    # Fallback: Find first {...} block greedily (may fail if there are braces in prose)
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        raise ValueError("No JSON object found in model output")
    return json.loads(m.group(0))


def _evidence_map(song_id: str, section_id: Optional[str], bars: Tuple[int, int]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Evidence pointers are *not* meant to be arbitrary SQL written by the model.
    They are canonical query templates/IDs that the verifier can check.
    """
    sb, eb = int(bars[0]), int(bars[1])
    sec = section_id or "S_global"
    return {
        "meter": [{
            "type": "sql",
            "tables": ["timesig_changes"],
            "sql": "SELECT num, den FROM timesig_changes WHERE song_id=? ORDER BY t_sec LIMIT 1",
            "params": [song_id],
        }],
        "tempo_bpm": [{
            "type": "sql",
            "tables": ["bars"],
            "sql": "SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?",
            "params": [song_id],
        }],
        "key": [{
            "type": "sql",
            "tables": ["key_changes"],
            "sql": "SELECT key FROM key_changes WHERE song_id=? ORDER BY at_bar, at_beat LIMIT 1",
            "params": [song_id],
        }],
        "roman": [{
            "type": "sql",
            "tables": ["chords"],
            "sql": "SELECT rn FROM chords WHERE song_id=? AND onset_bar BETWEEN ? AND ? ORDER BY onset_bar, onset_beat LIMIT 64",
            "params": [song_id, sb, eb],
        }],
        "progression": [{
            "type": "derived",
            "from_slots": ["roman", "key"],
            "bars": [sb, eb],
            "note": "progression is a formatted/optionally named Roman-numeral sequence derived from chords and key",
        }],
        "density": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='density' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "polyphony": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='polyphony' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "backbeat_strength": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='backbeat_strength' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "syncopation": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='syncopation' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "repeat_score_bar": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='repeat_score_bar' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "onset_entropy_16th": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='onset_entropy_16th' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "pitch_range": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='pitch_range' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "active_tracks": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='active_tracks' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "active_drums": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='active_drums' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "active_bass": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='active_bass' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "active_pad": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='active_pad' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "active_melody": [{
            "type": "sql",
            "tables": ["ts_bar"],
            "sql": "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature='active_melody' AND bar BETWEEN ? AND ?",
            "params": [song_id, sb, eb],
        }],
        "events": [{
            "type": "sql",
            "tables": ["events"],
            "sql": "SELECT bar, event_type, strength FROM events WHERE song_id=? AND bar BETWEEN ? AND ? ORDER BY bar, event_type LIMIT 50",
            "params": [song_id, sb, eb],
        }],
        "chord_summary_abs": [{
            "type": "sql",
            "tables": ["chords"],
            "sql": "SELECT onset_bar, onset_beat, root_pc, quality FROM chords WHERE song_id=? AND onset_bar BETWEEN ? AND ? ORDER BY onset_bar, onset_beat LIMIT 256",
            "params": [song_id, sb, eb],
        }],
        "instruments_summary": [{
            "type": "sql",
            "tables": ["notes", "tracks"],
            "sql": "SELECT t.gm_program, t.role, SUM(n.offset_sec - n.onset_sec) AS dur FROM notes n JOIN tracks t USING(song_id, track_id) WHERE n.song_id=? AND n.onset_bar BETWEEN ? AND ? GROUP BY t.gm_program, t.role ORDER BY dur DESC NULLS LAST LIMIT 12",
            "params": [song_id, sb, eb],
        }],
        "tags": [{
            "type": "sql",
            "tables": ["tags_section"],
            "sql": "SELECT tag, confidence FROM tags_section WHERE song_id=? AND section_id=? ORDER BY confidence DESC",
            "params": [song_id, sec],
        }],
        "texture_blurb": [{
            "type": "sql",
            "tables": ["notes", "tracks"],
            "sql": "SELECT t.name, COUNT(*) AS c FROM notes n JOIN tracks t USING(song_id, track_id) WHERE n.song_id=? AND n.onset_bar BETWEEN ? AND ? GROUP BY t.name ORDER BY c DESC LIMIT 3",
            "params": [song_id, sb, eb],
        }],
        "rhythm_trait": [{
            "type": "derived",
            "from_slots": ["syncopation", "backbeat_strength", "onset_entropy_16th"],
            "bars": [sb, eb],
        }],
    }


def _prompt_variants() -> Dict[str, Dict[str, str]]:
    return {
        "v1_concise": {
            "system": "You produce strict JSON only (no prose). Follow the schema exactly.",
            "user": (
                "Select up to {max_claims} claims from the provided slot values and produce:\n"
                "1) claims[] with exact slot/value pairs (do not invent values)\n"
                "2) caption_text: a single short sentence using only those claims\n"
                "Rules: Only use slots shown; if a value is null/empty, do not claim it; output valid JSON.\n"
            ),
        },
        "v2_evidence_focused": {
            "system": "Output strict JSON only. Prefer claims that are easy to verify from MIDI-derived tables.",
            "user": (
                "Choose up to {max_claims} claims. Prefer meter/tempo/key and rhythmic/structure summaries.\n"
                "Use exact values; do not infer genre/timbre.\n"
                "Output JSON with claims[] and caption_text.\n"
            ),
        },
        "v3_structure_first": {
            "system": "Strict JSON only.",
            "user": (
                "Choose up to {max_claims} claims prioritizing structure: meter, tempo, repeats, boundaries, rhythm traits.\n"
                "Use exact values provided. Output claims[] and caption_text.\n"
            ),
        },
    }


def _openai_chat_completion(
    api_key: str,
    base_url: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: int,
    seed: Optional[int],
) -> str:
    url = base_url.rstrip("/") + "/v1/chat/completions"
    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }
    if seed is not None:
        payload["seed"] = int(seed)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    return obj["choices"][0]["message"]["content"]


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
    payload: Dict[str, Any] = {
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


def _canonical_claims_from_slots(slots: Dict[str, Any], evidence: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    claims = []
    for slot, value in slots.items():
        if slot in ("section_id", "bars"):
            continue
        if value in (None, "", [], {}):
            continue
        claims.append({"slot": slot, "value": value, "evidence": evidence.get(slot, [])})
    # Keep deterministic ordering
    claims.sort(key=lambda c: c["slot"])
    return claims


def main() -> int:
    ap = argparse.ArgumentParser(description="LLM caption wrapper that outputs auditable JSON claims with evidence pointers")
    ap.add_argument("--db", required=True)
    ap.add_argument("--song_id", required=True)
    ap.add_argument("--section_id", default=None)
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--backend", choices=["none", "openai", "anthropic"], default=None, help="LLM backend; defaults to openai if OPENAI_API_KEY set, else anthropic if CLAUDE_API_KEY/ANTHROPIC_API_KEY set, else none")
    ap.add_argument("--model", default=None, help="Model name; defaults to OPENAI_MODEL or ANTHROPIC_MODEL depending on backend")
    ap.add_argument("--base_url", default=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com"))
    ap.add_argument("--anthropic_base_url", default=os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com"))
    ap.add_argument("--api_key_env", default=None, help="API key env var; defaults to OPENAI_API_KEY or CLAUDE_API_KEY/ANTHROPIC_API_KEY depending on backend")
    ap.add_argument("--temperature", type=float, default=0.2)
    ap.add_argument("--max_tokens", type=int, default=600)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--prompt_variant", default="v1_concise", help=f"One of: {', '.join(_prompt_variants().keys())}")
    ap.add_argument("--max_claims", type=int, default=10)
    args = ap.parse_args()

    if args.backend:
        backend = args.backend
    else:
        if os.environ.get("OPENAI_API_KEY"):
            backend = "openai"
        elif os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY"):
            backend = "anthropic"
        else:
            backend = "none"

    api_key_env = args.api_key_env
    if api_key_env is None:
        if backend == "openai":
            api_key_env = "OPENAI_API_KEY"
        elif backend == "anthropic":
            # Prefer CLAUDE_API_KEY; also supports ANTHROPIC_API_KEY for compatibility.
            api_key_env = "CLAUDE_API_KEY"
        else:
            api_key_env = "OPENAI_API_KEY"
    api_key = os.environ.get(api_key_env)
    if not api_key:
        api_key = _read_windows_env_registry(api_key_env)
    if not api_key and backend == "anthropic" and api_key_env != "ANTHROPIC_API_KEY":
        # Fallback for users who set ANTHROPIC_API_KEY instead of CLAUDE_API_KEY.
        api_key_env = "ANTHROPIC_API_KEY"
        api_key = os.environ.get(api_key_env) or _read_windows_env_registry(api_key_env)

    if args.model:
        model = args.model
    else:
        model = (
            os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
            if backend == "openai"
            else os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
        )

    con = connect(args.db)
    ensure_schema(con)

    slots = build_slots(con, args.song_id, section_id=args.section_id)
    bars = tuple(slots.get("bars") or (1, 1))
    evidence = _evidence_map(args.song_id, args.section_id, (int(bars[0]), int(bars[1])))

    canonical_claims = _canonical_claims_from_slots(slots, evidence)
    canonical_by_slot = {c["slot"]: c for c in canonical_claims}

    # Default caption text (no LLM) is deterministic and always available.
    default_caption = caption_short(slots)

    out: Dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "backend": backend,
        "model": (model if backend in ("openai", "anthropic") else None),
        "song_id": args.song_id,
        "section_id": args.section_id,
        "bars": [int(bars[0]), int(bars[1])],
        "prompt_variant": args.prompt_variant,
        "input_slots": [c["slot"] for c in canonical_claims],
        "claims": [],
        "caption_text": default_caption,
        "raw_model_output": None,
        "parse_ok": True,
        "parse_error": None,
    }

    if backend == "none":
        # Emit all canonical claims; no hallucinations possible in this mode.
        out["claims"] = canonical_claims[: args.max_claims]
        Path(args.out_json).write_text(json.dumps(out, indent=2), encoding="utf-8")
        con.close()
        return 0

    if not api_key:
        raise SystemExit(f"Missing API key env var: {api_key_env}")

    pv = _prompt_variants().get(args.prompt_variant)
    if not pv:
        raise SystemExit(f"Unknown prompt_variant: {args.prompt_variant}")

    # Provide the model a compact slot/value list and the allowed slot names.
    # Values are treated as ground truth; the model must not change them.
    slot_pack = [{"slot": c["slot"], "value": c["value"]} for c in canonical_claims]
    user_msg = pv["user"].format(max_claims=int(args.max_claims)) + "\nSLOTS_JSON:\n" + json.dumps(slot_pack, ensure_ascii=False)

    if backend == "openai":
        messages = [
            {"role": "system", "content": pv["system"]},
            {"role": "user", "content": user_msg},
            {"role": "user", "content": "Output schema: {schema, song_id, section_id, claims:[{slot,value}], caption_text}. Output JSON only."},
        ]
        raw = _openai_chat_completion(
            api_key=api_key,
            base_url=args.base_url,
            model=model,
            messages=messages,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            seed=args.seed,
        )
    else:
        # Anthropics Messages API does not support a seed.
        raw = _anthropic_messages(
            api_key=api_key,
            base_url=args.anthropic_base_url,
            model=model,
            system=pv["system"],
            user=user_msg + "\nOutput schema: {schema, song_id, section_id, claims:[{slot,value}], caption_text}. Output JSON only.",
            temperature=args.temperature,
            max_tokens=args.max_tokens,
        )
    out["raw_model_output"] = raw

    try:
        obj = _json_extract(raw)
    except Exception as e:
        # Keep the raw output for debugging; emit an empty claim list so downstream batch runs can continue.
        out["parse_ok"] = False
        out["parse_error"] = f"{type(e).__name__}: {e}"
        Path(args.out_json).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
        con.close()
        return 0
    claims_in = obj.get("claims", [])
    if not isinstance(claims_in, list):
        raise ValueError("claims must be a list")

    # Accept model-provided values, but attach canonical evidence pointers for verification.
    # If model uses unknown slots, keep them (they will fail verification later).
    claims_out: List[Dict[str, Any]] = []
    for c in claims_in[: int(args.max_claims)]:
        if not isinstance(c, dict):
            continue
        slot = c.get("slot")
        if not slot or not isinstance(slot, str):
            continue
        val = c.get("value")
        ev = canonical_by_slot.get(slot, {}).get("evidence", [])
        claims_out.append({"slot": slot, "value": val, "evidence": ev})

    out["claims"] = claims_out
    if isinstance(obj.get("caption_text"), str) and obj.get("caption_text"):
        out["caption_text"] = obj["caption_text"]

    Path(args.out_json).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
