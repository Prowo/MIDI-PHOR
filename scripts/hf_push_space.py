#!/usr/bin/env python3
"""
Create (if needed) and upload this repo to a Hugging Face Space (Docker SDK).

Requires:
  - pip install huggingface_hub
  - HF_TOKEN with WRITE access (classic "Write" token, or fine-grained with
    permission to create/write Spaces under your user/org).

Usage (from repo root):
  python scripts/hf_push_space.py
  python scripts/hf_push_space.py --repo-id StevenAu/MIDI-PHOR

Env:
  HF_TOKEN or HUGGING_FACE_HUB_TOKEN
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from huggingface_hub import HfApi


def main() -> int:
    ap = argparse.ArgumentParser(description="Push local tree to Hugging Face Space")
    ap.add_argument(
        "--repo-id",
        default=os.environ.get("HF_SPACE_REPO_ID", "StevenAu/MIDI-PHOR"),
        help="Space repo_id (default: StevenAu/MIDI-PHOR or HF_SPACE_REPO_ID)",
    )
    ap.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Project root to upload",
    )
    args = ap.parse_args()

    if not (os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")):
        print("Set HF_TOKEN (write-capable). See docs/DEMO.md / Hugging Face → Settings → Access Tokens.")
        return 1

    api = HfApi()
    try:
        me = api.whoami()
        name = me.get("name", me) if isinstance(me, dict) else me
        print(f"Authenticated as: {name}")
    except Exception as e:
        print("Auth failed:", e)
        return 1

    repo_id = args.repo_id
    root: Path = args.root

    try:
        url = api.create_repo(
            repo_id,
            repo_type="space",
            space_sdk="docker",
            private=False,
            exist_ok=True,
        )
        print("Space repo:", url)
    except Exception as e:
        print("create_repo failed (need WRITE token or create Space once in the web UI):", e)
        return 1

    # Exclude local dev artifacts (can be *gigabytes* — never upload to HF)
    ignore_patterns = [
        "**/.git/**",
        "**/__pycache__/**",
        "**/.venv/**",
        "**/venv/**",
        "**/env/**",
        "**/.idea/**",
        "**/.vscode/**",
        "**/cache/**",
        "**/data/**",
        "**/clean_midi/**",
        "**/clean_audio/**",
        "**/exports/**",
        "**/scorespec_json/**",
        "**/*.duckdb",
        "**/*.db",
        "**/*.sqlite",
        "**/.env",
        "**/.env.*",
        "**/*.wav",
    ]

    print("Uploading files (only source — excludes .venv, cache, data, etc.)...")
    api.upload_folder(
        folder_path=str(root),
        repo_id=repo_id,
        repo_type="space",
        ignore_patterns=ignore_patterns,
    )
    print(f"Done. Open: https://huggingface.co/spaces/{repo_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
