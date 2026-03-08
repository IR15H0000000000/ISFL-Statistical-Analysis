"""Player registry for cross-season identity linking.

Player names in PBP are consistently "Last, F." format. Cross-season linking
uses normalized name matching: same normalized name = same player (sim league
names are unique per player). Manual overrides via overrides.yaml handle edge
cases like name changes.

The registry can operate in two modes:
1. In-memory (default): Fast, no database required. Used during aggregation.
2. PostgreSQL-backed: Persistent, used by the storage layer.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_OVERRIDES_PATH = Path(__file__).parent / "overrides.yaml"


def _normalize(name: str) -> str:
    """Normalize a player name for matching.

    Strips trailing dots, normalizes whitespace around commas,
    and lowercases. This ensures 'Smith, J.' and 'Smith, J'
    map to the same key.
    """
    s = name.strip().lower()
    s = s.rstrip(".")
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s+", " ", s)
    return s


class PlayerRegistry:
    """In-memory player registry with cross-season linking."""

    def __init__(self) -> None:
        # name_key -> player_id
        self._name_to_id: dict[str, int] = {}
        # player_id -> canonical info
        self._players: dict[int, dict] = {}
        # player_id -> set of (name, season, team)
        self._aliases: dict[int, list[dict]] = {}
        self._next_id = 1
        # alias -> canonical normalized name (from overrides)
        self._overrides: dict[str, str] = {}
        self._load_overrides()

    def _load_overrides(self) -> None:
        if not _OVERRIDES_PATH.exists():
            return
        with open(_OVERRIDES_PATH) as f:
            data = yaml.safe_load(f)
        if not data or "merge" not in data:
            return
        for entry in data["merge"]:
            canonical = _normalize(entry["canonical"])
            for alias in entry.get("aliases", []):
                self._overrides[_normalize(alias)] = canonical

    def get_or_create(self, name: str, season: int, team: str | None = None) -> int:
        """Return the player_id for this name, creating if needed.

        Matching strategy:
        1. Exact normalized name match in existing registry
        2. Override file alias match
        3. Create new player
        """
        norm = _normalize(name)

        # Check overrides to resolve to canonical name
        resolved = self._overrides.get(norm, norm)

        # Check if we already have this name
        if resolved in self._name_to_id:
            pid = self._name_to_id[resolved]
            self._update_player(pid, name, season, team)
            # Also register the original name if it differs
            if norm != resolved and norm not in self._name_to_id:
                self._name_to_id[norm] = pid
            return pid

        # Create new player
        pid = self._next_id
        self._next_id += 1
        self._name_to_id[resolved] = pid
        if norm != resolved:
            self._name_to_id[norm] = pid
        self._players[pid] = {
            "canonical_name": name,
            "first_seen_season": season,
            "last_seen_season": season,
        }
        self._aliases[pid] = [{"name": name, "season": season, "team": team}]
        return pid

    def _update_player(self, pid: int, name: str, season: int, team: str | None) -> None:
        """Update an existing player's metadata."""
        p = self._players[pid]
        if season < p["first_seen_season"]:
            p["first_seen_season"] = season
        if season > p["last_seen_season"]:
            p["last_seen_season"] = season

        # Add alias if new
        alias = {"name": name, "season": season, "team": team}
        if alias not in self._aliases[pid]:
            self._aliases[pid].append(alias)

    def merge(self, keep_id: int, remove_id: int) -> None:
        """Merge two player entries, keeping keep_id."""
        if remove_id not in self._players or keep_id not in self._players:
            return

        # Move all aliases from remove to keep
        for alias in self._aliases.pop(remove_id, []):
            self._aliases[keep_id].append(alias)

        # Update name mappings
        for norm, pid in list(self._name_to_id.items()):
            if pid == remove_id:
                self._name_to_id[norm] = keep_id

        # Update season range
        removed = self._players.pop(remove_id)
        kept = self._players[keep_id]
        kept["first_seen_season"] = min(kept["first_seen_season"], removed["first_seen_season"])
        kept["last_seen_season"] = max(kept["last_seen_season"], removed["last_seen_season"])

    def get_player(self, player_id: int) -> dict | None:
        """Get player info by ID."""
        return self._players.get(player_id)

    def get_player_id(self, name: str) -> int | None:
        """Look up player ID by name."""
        norm = _normalize(name)
        resolved = self._overrides.get(norm, norm)
        return self._name_to_id.get(resolved)

    def get_aliases(self, player_id: int) -> list[dict]:
        """Get all known aliases for a player."""
        return self._aliases.get(player_id, [])

    def build_from_games(self, games) -> None:
        """Register all player names found in parsed games."""
        for game in games:
            for play in game.plays:
                team_abbr = self._play_team(game, play)
                for field in ("passer", "rusher", "receiver", "tackler",
                              "sacker", "interceptor", "kicker", "returner",
                              "fumbler", "fumble_recoverer"):
                    name = getattr(play, field, None)
                    if name:
                        self.get_or_create(name, game.season, team_abbr)

    @staticmethod
    def _play_team(game, play) -> str | None:
        """Determine team abbreviation for the possession team of a play."""
        tid = play.possession_team_id
        if tid is None:
            return None
        if tid == game.home_team_id:
            return game.home_team
        if tid == game.away_team_id:
            return game.away_team
        return None

    @property
    def player_count(self) -> int:
        return len(self._players)

    def all_players(self) -> list[dict]:
        """Return all players as a list of dicts."""
        result = []
        for pid, info in self._players.items():
            result.append({"player_id": pid, **info})
        return result
