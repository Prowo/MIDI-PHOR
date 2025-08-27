#!/usr/bin/env python3
"""
Utilities to normalize/clean music file names under clean_midi/.

Modes:
- rename-trailing (default): Remove trailing numeric suffixes separated by space.
  Example: "Song Title 2.mid" -> "Song Title.mid"
- delete-dot-duplicates: Delete files that end with .<number> before the extension
  but only if the corresponding base file exists.
  Example: "What's Up.1.mid" deleted if "What's Up.mid" exists.

Rules:
- Operates recursively under the clean_midi directory by default.
- Only targets common audio/MIDI extensions.
- Dry-run by default; use --apply to actually modify files.

Usage examples:
  python scripts/clean_trailing_numbers.py                       # rename-trailing dry-run
  python scripts/clean_trailing_numbers.py --apply               # rename-trailing apply
  python scripts/clean_trailing_numbers.py --mode delete-dot-duplicates
  python scripts/clean_trailing_numbers.py --mode delete-dot-duplicates --apply
  python scripts/clean_trailing_numbers.py --root clean_midi/Queen --extensions .mid --apply
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Iterable, List, Tuple


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


TRAILING_NUMBER_PATTERN = re.compile(r"^(?P<base>.*?)(?:\s+)(?P<num>\d+)$")
DOT_DUPLICATE_PATTERN = re.compile(r"^(?P<base>.*?)[.](?P<num>\d+)$")


def is_music_file(filename: str, extensions: Iterable[str]) -> bool:
    _, ext = os.path.splitext(filename)
    return ext.lower() in {e.lower() for e in extensions}


def propose_new_name(filename_no_ext: str) -> str | None:
    match = TRAILING_NUMBER_PATTERN.match(filename_no_ext)
    if not match:
        return None
    base = match.group("base").rstrip()
    return base if base else None


def find_candidates(root: str, extensions: Iterable[str]) -> List[Tuple[str, str]]:
    candidates: List[Tuple[str, str]] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if not is_music_file(name, extensions):
                continue
            stem, ext = os.path.splitext(name)
            new_stem = propose_new_name(stem)
            if new_stem is None:
                continue
            src = os.path.join(dirpath, name)
            dst = os.path.join(dirpath, f"{new_stem}{ext}")
            if os.path.normcase(src) == os.path.normcase(dst):
                # No change in case-insensitive filesystems
                continue
            candidates.append((src, dst))
    return candidates


def find_dot_number_duplicates(root: str, extensions: Iterable[str]) -> List[Tuple[str, str]]:
    """Return (dup_path, base_path) for files like "Name.1.mid" when "Name.mid" exists."""
    duplicates: List[Tuple[str, str]] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        # Build a set of existing stems per directory for quick membership checks
        stems_by_ext: dict[str, set[str]] = {}
        for name in filenames:
            if not is_music_file(name, extensions):
                continue
            stem, ext = os.path.splitext(name)
            stems_by_ext.setdefault(ext.lower(), set()).add(stem)

        for name in filenames:
            if not is_music_file(name, extensions):
                continue
            stem, ext = os.path.splitext(name)
            m = DOT_DUPLICATE_PATTERN.match(stem)
            if not m:
                continue
            base_stem = m.group("base")
            base_name = f"{base_stem}{ext}"
            if base_stem in stems_by_ext.get(ext.lower(), set()):
                dup_path = os.path.join(dirpath, name)
                base_path = os.path.join(dirpath, base_name)
                duplicates.append((dup_path, base_path))
    return duplicates


def rename_files(candidates: Iterable[Tuple[str, str]], apply: bool, overwrite: bool) -> Tuple[int, int, int]:
    performed = 0
    skipped = 0
    conflicts = 0
    for src, dst in candidates:
        if not apply:
            print(f"DRY-RUN rename: {src} -> {dst}")
            continue

        if os.path.exists(dst) and not overwrite:
            print(f"SKIP (exists): {dst}")
            conflicts += 1
            continue

        try:
            os.replace(src, dst)  # atomic where possible; overwrites if exists
            print(f"RENAMED: {src} -> {dst}")
            performed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: {src} -> {dst}: {exc}")
            skipped += 1
    return performed, skipped, conflicts


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean music filenames: rename trailing numbers or delete dot-number duplicates")
    parser.add_argument("--root", default=DEFAULT_ROOT, help="Root directory to scan (default: clean_midi)")
    parser.add_argument(
        "--extensions",
        nargs="*",
        default=DEFAULT_EXTENSIONS,
        help="File extensions to include (e.g., .mid .mp3). Default includes common audio/MIDI.",
    )
    parser.add_argument(
        "--mode",
        choices=["rename-trailing", "delete-dot-duplicates"],
        default="rename-trailing",
        help="Which cleaning operation to perform",
    )
    parser.add_argument("--apply", action="store_true", help="Perform changes (default is dry-run)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite if target exists")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    root = os.path.abspath(args.root)

    if not os.path.isdir(root):
        print(f"Root does not exist or is not a directory: {root}")
        return 2

    if args.mode == "rename-trailing":
        candidates = find_candidates(root, args.extensions)
        if not candidates:
            print("No files with trailing numbers found.")
            return 0
        print(f"Found {len(candidates)} file(s) with trailing numeric suffixes.")
        performed, skipped, conflicts = rename_files(candidates, apply=args.apply, overwrite=args.overwrite)
        if args.apply:
            print(f"Done. Renamed: {performed}, Skipped errors: {skipped}, Conflicts: {conflicts}")
        else:
            print("Dry-run complete. Use --apply to perform changes.")
    else:  # delete-dot-duplicates
        duplicates = find_dot_number_duplicates(root, args.extensions)
        if not duplicates:
            print("No dot-number duplicates found.")
            return 0
        print(f"Found {len(duplicates)} dot-number duplicate file(s).")
        deleted = 0
        errors = 0
        for dup_path, base_path in duplicates:
            if args.apply:
                try:
                    os.remove(dup_path)
                    print(f"DELETED: {dup_path} (base exists: {base_path})")
                    deleted += 1
                except Exception as exc:  # noqa: BLE001
                    print(f"ERROR delete: {dup_path}: {exc}")
                    errors += 1
            else:
                print(f"DRY-RUN delete: {dup_path} (base exists: {base_path})")
        if args.apply:
            print(f"Done. Deleted: {deleted}, Errors: {errors}")
        else:
            print("Dry-run complete. Use --apply to delete.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


