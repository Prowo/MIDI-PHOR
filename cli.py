# cli.py
from __future__ import annotations
import argparse, os
from pathlib import Path

from db.duck import connect, ensure_schema  # assumes you created db/duck.py earlier
from extractors import symbolic  # your existing/earlier symbolic extractor module
from extractors import audio as audio_ext
from extractors import graph as graph_ext
from assemble.section_merge import merge_for_song
from assemble.caption import caption_for_song, captions_by_section

def main():
    ap = argparse.ArgumentParser(description="MusicCap pipeline")
    ap.add_argument("--db", default="data/musiccap.duckdb", help="DuckDB path")
    ap.add_argument("--midi_glob", required=True, help="Glob e.g. 'data/**/*.mid'")
    ap.add_argument("--sf2", default=None, help="SoundFont (.sf2) for fluidsynth (optional)")
    ap.add_argument("--render_dir", default="cache", help="Where to write WAV renders")
    ap.add_argument("--skip_audio", action="store_true", help="Skip audio rendering/features")
    args = ap.parse_args()

    con = connect(args.db)
    ensure_schema(con)

    midi_files = sorted(Path().glob(args.midi_glob))
    if not midi_files:
        print("No files matched.")
        return

    os.makedirs(args.render_dir, exist_ok=True)

    for midi in midi_files:
        song_id = midi.stem
        print(f"\n=== {song_id} ===")

        # 1) Symbolic
        try:
            symbolic.run(song_id, str(midi), con)  # from the symbolic module you already have
            print("Symbolic: OK")
        except Exception as e:
            print("Symbolic failed:", e); continue

        # 2) Audio (optional)
        if not args.skip_audio:
            try:
                cfg = audio_ext.AudioConfig(soundfont_path=args.sf2)
                wav_out = str(Path(args.render_dir) / f"{song_id}.wav")
                audio_ext.run(song_id, str(midi), con, wav_out=wav_out, cfg=cfg)
                print("Audio: OK")
            except Exception as e:
                print("Audio failed:", e)

        # 3) Merge sections
        try:
            merge_for_song(con, song_id)
            print("Sections merged.")
        except Exception as e:
            print("Section merge failed:", e)

        # 4) Graph
        try:
            graph_ext.run(song_id, con)
            print("Graph: OK")
        except Exception as e:
            print("Graph failed:", e)

        # 5) Captions
        try:
            print("Song caption:", caption_for_song(con, song_id))
            for sid, txt in captions_by_section(con, song_id, mode="short"):
                print(f"  {sid}: {txt}")
        except Exception as e:
            print("Captioning failed:", e)

if __name__ == "__main__":
    main()
