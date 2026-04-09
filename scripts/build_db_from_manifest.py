#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import sys
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.duck import connect, ensure_schema
from extractors import symbolic
from extractors import audio as audio_ext
from extractors import graph as graph_ext
from assemble.section_merge import merge_for_song


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a DuckDB for a fixed manifest of MIDI files")
    ap.add_argument("--manifest", required=True, help="Path to exports/eval_sets/eval*.json")
    ap.add_argument("--db", required=True, help="Output DuckDB path")
    ap.add_argument(
        "--audio_mode",
        choices=["symbolic_only", "audio_original_if_provided", "audio_synth_render"],
        default="symbolic_only",
    )
    ap.add_argument("--sf2", default=None)
    ap.add_argument("--render_dir", default="cache")
    ap.add_argument("--limit", type=int, default=None, help="Optional limit for debugging")
    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    items: List[Dict[str, str]] = list(m.get("items") or [])
    if args.limit is not None:
        items = items[: int(args.limit)]

    con = connect(args.db)
    ensure_schema(con)

    ok = 0
    fail = 0
    t0 = time.time()
    for idx, it in enumerate(items, start=1):
        song_id = it["song_id"]
        midi_path = str(Path(it["midi_path"]))
        print(f"[{idx}/{len(items)}] {song_id}")
        try:
            symbolic.run(song_id, midi_path, con)
            if args.audio_mode != "symbolic_only":
                cfg = audio_ext.AudioConfig(soundfont_path=args.sf2)
                wav_out = str(Path(args.render_dir) / f"{song_id}.wav")
                audio_ext.run(song_id, midi_path, con, wav_out=wav_out, audio_in=None, cfg=cfg)
            merge_for_song(con, song_id)
            graph_ext.run(song_id, con)
            ok += 1
        except Exception as e:
            fail += 1
            print(f"[FAIL] {song_id}: {e}")
        if idx % 10 == 0:
            dt = time.time() - t0
            rate = dt / max(1, idx)
            eta = rate * (len(items) - idx)
            print(f"progress ok={ok} fail={fail} avg_s_per_song={rate:.2f} eta_min={eta/60.0:.1f}")
    con.close()
    print(f"ok={ok} fail={fail} db={args.db}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
