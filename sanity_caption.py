#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional

from db.duck import connect, ensure_schema
from extractors import symbolic
from extractors import audio as audio_ext
from extractors import graph as graph_ext
from assemble.section_merge import merge_for_song
from assemble.caption import caption_for_song, captions_by_section


def run_pipeline_if_needed(song_id: str, midi_path: Optional[str], con, skip_audio: bool, sf2: Optional[str], render_dir: str) -> None:
    if not midi_path:
        return
    # Symbolic
    symbolic.run(song_id, midi_path, con)
    # Audio (optional)
    if not skip_audio:
        cfg = audio_ext.AudioConfig(soundfont_path=sf2)
        wav_out = str(Path(render_dir) / f"{song_id}.wav")
        audio_ext.run(song_id, midi_path, con, wav_out=wav_out, cfg=cfg)
    # Sections (merged)
    merge_for_song(con, song_id)
    # Graph
    graph_ext.run(song_id, con)


def summarize_ts_features(con, song_id: str) -> str:
    # List features present and simple aggregates for common ones
    rows = con.execute(
        """
        SELECT feature, COUNT(*) AS n
        FROM ts_bar WHERE song_id=?
        GROUP BY feature ORDER BY feature
        """,
        [song_id]
    ).fetchall()
    features = ", ".join([f"{f}({n})" for f, n in rows]) if rows else "<none>"

    def avg_feat(feat: str) -> Optional[float]:
        r = con.execute(
            "SELECT AVG(value) FROM ts_bar WHERE song_id=? AND feature=?",
            [song_id, feat]
        ).fetchone()
        return float(r[0]) if r and r[0] is not None else None

    keys = [
        "density",
        "polyphony",
        "backbeat_strength",
        "syncopation",
        "energy_bar_z",
        "brightness_bar_z",
        "repeat_score_bar",
    ]
    avgs = []
    for k in keys:
        v = avg_feat(k)
        if v is not None:
            avgs.append(f"{k}≈{v:.2f}")
    return f"features: {features}\navg: " + (", ".join(avgs) if avgs else "<n/a>")


def summarize_graph(con, song_id: str) -> str:
    nodes = con.execute("SELECT COUNT(*) FROM graph_nodes WHERE song_id=?", [song_id]).fetchone()[0]
    edges = con.execute("SELECT COUNT(*) FROM graph_edges WHERE song_id=?", [song_id]).fetchone()[0]
    rel_rows = con.execute(
        "SELECT rel_type, COUNT(*) FROM graph_edges WHERE song_id=? GROUP BY rel_type ORDER BY rel_type",
        [song_id]
    ).fetchall()
    rels = ", ".join([f"{r}:{c}" for r, c in rel_rows]) if rel_rows else "<none>"
    return f"nodes={nodes}, edges={edges} [{rels}]"


def main():
    ap = argparse.ArgumentParser(description="Sanity caption and summaries")
    ap.add_argument("--db", default="data/musiccap.duckdb")
    ap.add_argument("--midi", default=None, help="Path to a single MIDI (optional)")
    ap.add_argument("--song_id", default=None, help="Song ID (defaults to MIDI stem if --midi set)")
    ap.add_argument("--skip_audio", action="store_true")
    ap.add_argument("--sf2", default=None)
    ap.add_argument("--render_dir", default="cache")
    args = ap.parse_args()

    con = connect(args.db)
    ensure_schema(con)

    midi_path = str(Path(args.midi)) if args.midi else None
    song_id = args.song_id or (Path(args.midi).stem if args.midi else None)
    if not song_id:
        raise SystemExit("Provide --midi or --song_id")

    # Run pipeline pieces if a MIDI is provided
    run_pipeline_if_needed(song_id, midi_path, con, args.skip_audio, args.sf2, args.render_dir)

    # Captions
    print(f"\n=== {song_id} ===")
    print("Global:", caption_for_song(con, song_id))
    for sid, txt in captions_by_section(con, song_id, mode="short"):
        print(f"  {sid}: {txt}")

    # TS summary
    print("\n[ts_bar]", summarize_ts_features(con, song_id))

    # Graph summary
    print("[graph]", summarize_graph(con, song_id))


if __name__ == "__main__":
    main()


