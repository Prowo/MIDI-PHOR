#!/usr/bin/env python3
"""Write a tiny GM piano demo (original, public-domain intent) for the Gradio Examples."""
from __future__ import annotations

from pathlib import Path

import mido
from mido import Message, MetaMessage, MidiFile, MidiTrack


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out = root / "examples" / "demo_scale.mid"
    out.parent.mkdir(parents=True, exist_ok=True)

    mid = MidiFile(type=1)
    track = MidiTrack()
    mid.tracks.append(track)

    track.append(MetaMessage("track_name", name="MIDIPHOR demo", time=0))
    track.append(MetaMessage("set_tempo", tempo=mido.bpm2tempo(100), time=0))
    track.append(
        MetaMessage("time_signature", numerator=4, denominator=4, clocks_per_click=24, notated_32nd_notes_per_beat=8, time=0)
    )
    track.append(Message("program_change", channel=0, program=0, time=0))

    q = mid.ticks_per_beat // 2
    pitches = [60, 62, 64, 65, 67, 69, 71, 72]
    for i, p in enumerate(pitches):
        track.append(Message("note_on", channel=0, note=p, velocity=82, time=0 if i == 0 else 0))
        track.append(Message("note_off", channel=0, note=p, velocity=0, time=q))

    mid.save(str(out))
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
