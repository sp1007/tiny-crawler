"""Misc helpers."""
import hashlib
from pathlib import Path
from typing import Union


def safe_filename(seed: str, suffix: str = "") -> str:
    """Hash a string to produce a safe filename."""
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return f"{digest}{suffix}"


def ensure_dir(path: Union[str, Path]) -> Path:
    """Create directory if missing and return Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
