# utils/ids.py
from __future__ import annotations
import hashlib, uuid
from typing import Iterable

def _base36(n: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0: return "0"
    s = []
    while n:
        n, r = divmod(n, 36)
        s.append(chars[r])
    return "".join(reversed(s))

def deterministic_id(prefix: str, parts: Iterable[str], length: int = 12) -> str:
    """
    Build a stable ID from arbitrary parts.
    Example: deterministic_id("ch", [song_id, str(bar), str(beat)])
    """
    h = hashlib.blake2b("|".join(parts).encode("utf-8"), digest_size=16).hexdigest()
    return f"{prefix}{h[:length]}"

def random_id(prefix: str, length: int = 12) -> str:
    """
    Random short ID with prefix (uuid4-based).
    """
    return f"{prefix}{uuid.uuid4().hex[:length]}"

def short_md5(s: str, length: int = 10) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:length]
