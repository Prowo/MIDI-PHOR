#!/usr/bin/env python3
"""
Count music files under clean_midi/ (recursively) and print totals per extension and overall.

Usage:
  python scripts/count_songs.py
  python scripts/count_songs.py --root clean_midi --extensions .mid .mp3
"""

from __future__ import annotations

import argparse
import os
from collections import Counter, defaultdict
from typing import Dict, Iterable, List


DEFAULT_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "clean_midi")
DEFAULT_EXTENSIONS = [
    ".mid",
    ".midi",
    ".kar",
    ".mp3",
    ".wav",
    ".flac",
    ".ogg",
    ".m4a",
    ".aac",
    ".wma",
]


def is_music_file(filename: str, extensions: Iterable[str]) -> bool:
    _, ext = os.path.splitext(filename)
    return ext.lower() in {e.lower() for e in extensions}


def count_music_files(root: str, extensions: Iterable[str]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if is_music_file(name, extensions):
                _, ext = os.path.splitext(name)
                counts[ext.lower()] += 1
    return dict(counts)


def parse_args(argv: List[str]):
    parser = argparse.ArgumentParser(description="Count music files under a directory")
    parser.add_argument("--root", default=DEFAULT_ROOT, help="Root directory to scan (default: clean_midi)")
    parser.add_argument(
        "--extensions",
        nargs="*",
        default=DEFAULT_EXTENSIONS,
        help="File extensions to include (e.g., .mid .mp3). Default includes common audio/MIDI.",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    root = os.path.abspath(args.root)
    if not os.path.isdir(root):
        print(f"Root does not exist or is not a directory: {root}")
        return 2

    counts = count_music_files(root, args.extensions)
    total = sum(counts.values())

    if not counts:
        print("No music files found.")
        return 0

    print(f"Counting music files under: {root}")
    for ext in sorted(counts.keys()):
        print(f"{ext}: {counts[ext]}")
    print("-" * 24)
    print(f"Total: {total}")
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))


