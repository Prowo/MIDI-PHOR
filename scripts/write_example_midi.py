#!/usr/bin/env python3
"""
Write a short original multi-track MIDI for the Gradio example.

Named \"demo_abba\" as a **demo label only** — this is NOT an ABBA catalog recording
or a transcription of their songs; it is a simple original pop-style sketch (euro-disco-ish
groove + I–V–vi–IV-style harmony) for pipeline testing.
"""
from __future__ import annotations

from pathlib import Path

import mido
from mido import Message, MetaMessage, MidiFile, MidiTrack


def _named_track(name: str) -> MidiTrack:
    t = MidiTrack()
    t.append(MetaMessage("track_name", name=name, time=0))
    return t


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out = root / "examples" / "demo_abba.mid"
    out.parent.mkdir(parents=True, exist_ok=True)

    mid = MidiFile(type=1)
    tpb = 480
    mid.ticks_per_beat = tpb
    beat = tpb
    bar = 4 * beat

    # Meta only
    meta = MidiTrack()
    meta.append(MetaMessage("track_name", name="MIDIPHOR demo", time=0))
    meta.append(MetaMessage("set_tempo", tempo=mido.bpm2tempo(124), time=0))
    meta.append(
        MetaMessage(
            "time_signature",
            numerator=4,
            denominator=4,
            clocks_per_click=24,
            notated_32nd_notes_per_beat=8,
            time=0,
        )
    )
    mid.tracks.append(meta)

    # Ch 0 — bright piano hook (original melody fragment)
    melo = _named_track("Demo hook")
    melo.append(Message("program_change", channel=0, program=0, time=0))
    hook = [72, 72, 74, 76, 74, 72, 69, 72]
    for i, p in enumerate(hook):
        melo.append(Message("note_on", channel=0, note=p, velocity=88, time=0 if i == 0 else 0))
        melo.append(Message("note_off", channel=0, note=p, velocity=0, time=beat // 2))
    for p in [76, 74, 72, 69]:
        melo.append(Message("note_on", channel=0, note=p, velocity=84, time=0))
        melo.append(Message("note_off", channel=0, note=p, velocity=0, time=beat // 2))
    mid.tracks.append(melo)

    # Ch 1 — synth pad: chord tones as stacked notes (simple mido deltas)
    pad = _named_track("Pad (demo harmony)")
    pad.append(Message("program_change", channel=1, program=88, time=0))
    progression = [
        [60, 64, 67],
        [67, 71, 74],
        [69, 72, 76],
        [65, 69, 72],
    ]
    hold = 2 * bar
    for ci, pcs in enumerate(progression):
        dt = 0 if ci == 0 else 0
        for j, p in enumerate(pcs):
            pad.append(Message("note_on", channel=1, note=p, velocity=56, time=dt if j == 0 else 0))
        pad.append(Message("note_off", channel=1, note=pcs[0], velocity=0, time=hold))
        for p in pcs[1:]:
            pad.append(Message("note_off", channel=1, note=p, velocity=0, time=0))
    mid.tracks.append(pad)

    # Ch 2 — bass roots (original pattern)
    bass = _named_track("Bass")
    bass.append(Message("program_change", channel=2, program=32, time=0))
    roots = [36, 43, 33, 41] * 2  # C G A F x2
    for i, r in enumerate(roots):
        bass.append(Message("note_on", channel=2, note=r, velocity=96, time=0 if i == 0 else 0))
        bass.append(Message("note_off", channel=2, note=r, velocity=0, time=bar))
    mid.tracks.append(bass)

    mid.save(str(out))
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
