# cli.py
from __future__ import annotations
import argparse, os
from pathlib import Path

from db.duck import connect, ensure_schema  # assumes you created db/duck.py earlier
from extractors import symbolic  # your existing/earlier symbolic extractor module
from extractors import audio as audio_ext
from extractors import graph as graph_ext
from assemble.section_merge import merge_for_song
from assemble.caption import caption_for_song, captions_by_section, caption_medium, slots_for_section

def main():
    ap = argparse.ArgumentParser(description="MusicCap pipeline")
    ap.add_argument("--db", default="data/musiccap.duckdb", help="DuckDB path")
    ap.add_argument("--midi_glob", required=False, help="Glob e.g. 'data/**/*.mid'")
    ap.add_argument("--audio_glob", required=False, help="Glob e.g. 'data/**/*.wav' (original audio)")
    ap.add_argument("--sf2", default=None, help="SoundFont (.sf2) for fluidsynth (optional)")
    ap.add_argument("--render_dir", default="cache", help="Where to write WAV renders")
    ap.add_argument("--skip_audio", action="store_true", help="Skip audio rendering/features")
    ap.add_argument("--caption_mode", choices=["short","medium"], default="short", help="Caption detail level")
    args = ap.parse_args()

    con = connect(args.db)
    ensure_schema(con)

    midi_files = sorted(Path().glob(args.midi_glob)) if args.midi_glob else []
    audio_files = sorted(Path().glob(args.audio_glob)) if args.audio_glob else []
    if not midi_files and not audio_files:
        print("No files matched (MIDI or audio).")
        return

    os.makedirs(args.render_dir, exist_ok=True)

    # Build a song list from both sources by stem
    by_stem = {}
    for p in midi_files:
        by_stem.setdefault(p.stem, {})['midi'] = p
    for p in audio_files:
        by_stem.setdefault(p.stem, {})['audio'] = p

    for stem, rec in by_stem.items():
        song_id = stem
        print(f"\n=== {song_id} ===")

        # 1) Symbolic
        midi_path = str(rec['midi']) if 'midi' in rec else None
        if midi_path:
            try:
                symbolic.run(song_id, midi_path, con)
                print("Symbolic: OK")
            except Exception as e:
                print("Symbolic failed:", e)

        # 2) Audio (optional)
        if not args.skip_audio:
            try:
                cfg = audio_ext.AudioConfig(soundfont_path=args.sf2)
                wav_out = str(Path(args.render_dir) / f"{song_id}.wav")
                audio_in = str(rec['audio']) if 'audio' in rec else None
                audio_ext.run(song_id, midi_path, con, wav_out=wav_out, audio_in=audio_in, cfg=cfg)
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
            if args.caption_mode == "medium":
                # Build medium caption for whole song
                slots = slots_for_section(con, song_id, section_id=None)
                song_txt = caption_medium(slots)
            else:
                song_txt = caption_for_song(con, song_id)
            print("Song caption:", song_txt)
            for sid, txt in captions_by_section(con, song_id, mode=args.caption_mode):
                print(f"  {sid}: {txt}")
        except Exception as e:
            print("Captioning failed:", e)

if __name__ == "__main__":
    main()
