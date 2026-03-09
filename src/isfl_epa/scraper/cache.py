"""Local JSON file cache for scraped data.

Each season gets a directory under ``data/raw/{LEAGUE}_S{season}/`` with
individual JSON files for each data type (pbp, boxscore, roster, etc.).

A ``_meta.json`` sidecar file in each season directory tracks fetch
timestamps and file sizes so users can see cache freshness without
re-downloading.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from isfl_epa.config import League

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "raw"

_SEASON_DIR_RE = re.compile(r"^([A-Z]+)_S(\d+)$")


def _cache_path(league: League, season: int, data_type: str, file_num: int) -> Path:
    return DATA_DIR / f"{league.value}_S{season}" / f"{data_type}_{file_num}.json"


def _meta_path(league: League, season: int) -> Path:
    return DATA_DIR / f"{league.value}_S{season}" / "_meta.json"


def get_cached(league: League, season: int, data_type: str, file_num: int) -> list[dict] | None:
    path = _cache_path(league, season, data_type, file_num)
    if path.exists():
        return json.loads(path.read_text())
    return None


def save_to_cache(league: League, season: int, data_type: str, file_num: int, data: list[dict]) -> None:
    path = _cache_path(league, season, data_type, file_num)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(data)
    path.write_text(content)

    # Update metadata sidecar
    _update_meta(league, season, data_type, file_num, len(content.encode()))


def _update_meta(league: League, season: int, data_type: str, file_num: int, size_bytes: int) -> None:
    """Write or update the fetch timestamp for a cached file in the sidecar."""
    meta_file = _meta_path(league, season)
    meta = _read_meta(meta_file)
    key = f"{data_type}_{file_num}"
    meta[key] = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "size_bytes": size_bytes,
    }
    meta_file.write_text(json.dumps(meta, indent=2))


def _read_meta(meta_file: Path) -> dict:
    """Read the metadata sidecar, returning {} on any error."""
    if meta_file.exists():
        try:
            return json.loads(meta_file.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------


def get_cache_metadata(league: League, season: int) -> dict:
    """Return the metadata dict for a season's cache, or {} if none exists."""
    return _read_meta(_meta_path(league, season))


def get_file_age_hours(league: League, season: int, data_type: str, file_num: int) -> float | None:
    """Return how many hours ago a cached file was fetched, or None if unknown."""
    meta = get_cache_metadata(league, season)
    key = f"{data_type}_{file_num}"
    entry = meta.get(key)
    if not entry or "fetched_at" not in entry:
        return None
    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
        delta = datetime.now(timezone.utc) - fetched
        return delta.total_seconds() / 3600
    except (ValueError, TypeError):
        return None


def list_cached_seasons(league: League) -> list[int]:
    """Return sorted list of season numbers that have cached data for this league."""
    if not DATA_DIR.exists():
        return []
    seasons = []
    for d in DATA_DIR.iterdir():
        if d.is_dir():
            m = _SEASON_DIR_RE.match(d.name)
            if m and m.group(1) == league.value:
                seasons.append(int(m.group(2)))
    return sorted(seasons)


def get_season_cache_summary(league: League, season: int) -> dict:
    """Return per-file cache info for a season.

    Returns:
        Dict with keys:
        - ``files``: list of dicts with ``name``, ``size_bytes``, ``fetched_at``
        - ``total_size_bytes``: sum of all file sizes
        - ``file_count``: number of cached files
    """
    season_dir = DATA_DIR / f"{league.value}_S{season}"
    if not season_dir.exists():
        return {"files": [], "total_size_bytes": 0, "file_count": 0}

    meta = get_cache_metadata(league, season)
    files = []
    total_size = 0

    for path in sorted(season_dir.iterdir()):
        if path.name == "_meta.json" or not path.is_file():
            continue
        size = path.stat().st_size
        total_size += size

        key = path.stem  # e.g. "pbp_1"
        entry = meta.get(key, {})
        fetched_at = entry.get("fetched_at")
        if not fetched_at:
            # Fallback to file mtime for legacy caches
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            fetched_at = mtime.isoformat()

        files.append({
            "name": path.name,
            "size_bytes": size,
            "fetched_at": fetched_at,
        })

    return {
        "files": files,
        "total_size_bytes": total_size,
        "file_count": len(files),
    }


def clear_season_cache(league: League, season: int, data_type: str | None = None) -> int:
    """Delete cached files for a season, optionally filtered by data type.

    Args:
        league: League to clear cache for.
        season: Season number.
        data_type: If provided, only delete files matching this prefix (e.g. "pbp", "boxscore").

    Returns:
        Number of files deleted.
    """
    season_dir = DATA_DIR / f"{league.value}_S{season}"
    if not season_dir.exists():
        return 0

    deleted = 0
    for path in list(season_dir.iterdir()):
        if path.name == "_meta.json" or not path.is_file():
            continue
        if data_type and not path.stem.startswith(data_type):
            continue
        path.unlink()
        deleted += 1

    # Update metadata sidecar: remove entries for deleted files
    meta_file = _meta_path(league, season)
    if meta_file.exists():
        if data_type:
            meta = _read_meta(meta_file)
            meta = {k: v for k, v in meta.items() if not k.startswith(data_type)}
            if meta:
                meta_file.write_text(json.dumps(meta, indent=2))
            else:
                meta_file.unlink(missing_ok=True)
        else:
            meta_file.unlink(missing_ok=True)

    # Remove empty season directory
    if season_dir.exists() and not any(season_dir.iterdir()):
        season_dir.rmdir()

    return deleted
