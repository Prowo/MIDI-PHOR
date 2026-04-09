#!/usr/bin/env python
from __future__ import annotations
import argparse
import os
import sys

import numpy as np
import pretty_midi as pm
import soundfile as sf


def render(midi_path: str, out_path: str, sr: int = 44100, sf2: str | None = None, gain_db: float = 0.0) -> str:
    midi = pm.PrettyMIDI(midi_path)
    if sf2:
        audio = midi.fluidsynth(fs=sr, sf2_path=sf2)
    else:
        audio = midi.synthesize(fs=sr)
    if np.max(np.abs(audio)) > 0:
        audio = audio / np.max(np.abs(audio))
    if gain_db != 0.0:
        audio = audio * (10 ** (gain_db / 20.0))
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    sf.write(out_path, audio, sr)
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description="Render MIDI to WAV")
    ap.add_argument("midi", help="Input MIDI file path")
    ap.add_argument("out", help="Output WAV file path")
    ap.add_argument("--sr", type=int, default=44100)
    ap.add_argument("--sf2", type=str, default=None)
    ap.add_argument("--gain_db", type=float, default=0.0)
    args = ap.parse_args()
    try:
        out_path = render(args.midi, args.out, sr=args.sr, sf2=args.sf2, gain_db=args.gain_db)
        print(out_path)
        return 0
    except Exception as e:
        print(f"Render failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


