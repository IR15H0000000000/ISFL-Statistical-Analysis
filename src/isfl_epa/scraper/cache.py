import json
from pathlib import Path

from isfl_epa.config import League

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "raw"


def _cache_path(league: League, season: int, data_type: str, file_num: int) -> Path:
    return DATA_DIR / f"{league.value}_S{season}" / f"{data_type}_{file_num}.json"


def get_cached(league: League, season: int, data_type: str, file_num: int) -> list[dict] | None:
    path = _cache_path(league, season, data_type, file_num)
    if path.exists():
        return json.loads(path.read_text())
    return None


def save_to_cache(league: League, season: int, data_type: str, file_num: int, data: list[dict]) -> None:
    path = _cache_path(league, season, data_type, file_num)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))
