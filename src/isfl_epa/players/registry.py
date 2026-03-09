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

import logging
import re
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_OVERRIDES_PATH = Path(__file__).parent / "overrides.yaml"


def _strip_tags(name: str) -> str:
    """Remove parenthetical tags (C), (R), (BOT) etc., preserving case and format.

    'Penix (C) (R), P.' -> 'Penix, P.'
    'Abstract Geometry (R)' -> 'Abstract Geometry'
    """
    s = re.sub(r"\s*\([^)]*\)", "", name)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_special(name: str) -> str:
    """Remove trademark, copyright, and other non-alphanumeric decorators."""
    return re.sub(r"[™®©]", "", name)


def _normalize(name: str) -> str:
    """Normalize a player name for matching.

    Strips parenthetical tags like (C), (R), (BOT), special chars (™),
    trailing dots, normalizes whitespace around commas, and lowercases.
    This ensures 'Penix (C) (R), P.' and 'Penix, P' map to the same key.
    """
    s = _strip_tags(name)
    s = _strip_special(s)
    s = s.lower()
    s = s.rstrip(".")
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _to_last_first_key(name: str) -> str | None:
    """Convert 'First Last' format to 'last, f' for cross-format matching.

    The sim PBP convention is 'LastName, F.' where F is the first name initial.
    For multi-word names, the LAST word is treated as the last name:
      'Jean Claude Goddamn' -> 'goddamn, j'
      'Peaquod Von Turbo'   -> 'von turbo, p'

    We try two strategies:
    1. Last word only as last name (matches PBP convention for most names)
    2. All words after first as last name (matches multi-word last names)

    Returns None if already in 'Last, F.' format or can't be parsed.
    """
    s = _strip_tags(name)
    s = _strip_special(s)
    if "," in s:
        return None  # Already "Last, F." format
    parts = s.split()
    if len(parts) < 2:
        return None
    first_initial = parts[0][0].lower()
    # Primary: all-after-first as last name (for "Von Turbo, P." style)
    # Preserve internal dots (e.g., "Jr.") — _normalize handles final dot stripping
    last_full = " ".join(parts[1:]).lower()
    # Only strip trailing dot if it's NOT part of a suffix like "Jr."
    if last_full.endswith(".") and not last_full.endswith("jr."):
        last_full = last_full.rstrip(".")
    return f"{last_full}, {first_initial}"


def _to_last_first_key_short(name: str) -> str | None:
    """Convert 'First Middle Last' to 'last, f' using only the last word.

    Handles cases like 'Jean Claude Goddamn' -> 'goddamn, j' where PBP
    only uses the final word as the last name.
    """
    s = _strip_tags(name)
    s = _strip_special(s)
    s = s.rstrip(".")
    if "," in s:
        return None
    parts = s.split()
    if len(parts) < 3:
        return None  # Only useful for 3+ word names
    first_initial = parts[0][0].lower()
    last_word = parts[-1].lower()
    return f"{last_word}, {first_initial}"


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
        3. Cross-format match ("First Last" ↔ "Last, F.")
        4. Create new player
        """
        clean = _strip_special(_strip_tags(name))
        norm = _normalize(name)

        # Check overrides to resolve to canonical name
        resolved = self._overrides.get(norm, norm)
        if resolved != norm:
            logger.debug("Override resolved '%s' -> '%s'", norm, resolved)

        # Check if we already have this name
        if resolved in self._name_to_id:
            pid = self._name_to_id[resolved]
            self._update_player(pid, clean, season, team)
            if norm != resolved and norm not in self._name_to_id:
                self._name_to_id[norm] = pid
            return pid

        # Cross-format match: "Peaquod Von Turbo" ↔ "Von Turbo, P."
        alt_key = _to_last_first_key(name)
        if alt_key and alt_key in self._name_to_id:
            pid = self._name_to_id[alt_key]
            self._update_player(pid, clean, season, team)
            self._name_to_id[norm] = pid
            return pid

        # Short cross-format: "Jean Claude Goddamn" ↔ "Goddamn, J."
        short_key = _to_last_first_key_short(name)
        if short_key and short_key in self._name_to_id:
            pid = self._name_to_id[short_key]
            self._update_player(pid, clean, season, team)
            self._name_to_id[norm] = pid
            return pid

        # Create new player
        pid = self._next_id
        self._next_id += 1
        logger.debug("New player id=%d name='%s' season=%d", pid, clean, season)
        self._name_to_id[resolved] = pid
        if norm != resolved:
            self._name_to_id[norm] = pid
        self._players[pid] = {
            "canonical_name": clean,
            "first_seen_season": season,
            "last_seen_season": season,
        }
        self._aliases[pid] = [{"name": clean, "season": season, "team": team}]
        return pid

    def _update_player(self, pid: int, name: str, season: int, team: str | None) -> None:
        """Update an existing player's metadata.

        Note: ``name`` should already be tag-stripped by the caller.
        """
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
                off_team = self._play_team(game, play)
                def_team = self._non_possession_team(game, play)
                # Offensive players: possession team
                for field in ("passer", "rusher", "receiver", "kicker"):
                    name = getattr(play, field, None)
                    if name:
                        self.get_or_create(name, game.season, off_team)
                # Defensive players: non-possession team
                for field in ("tackler", "sacker", "interceptor"):
                    name = getattr(play, field, None)
                    if name:
                        self.get_or_create(name, game.season, def_team)
                # Returner: receiving team (non-possession on kick plays)
                if play.returner:
                    self.get_or_create(play.returner, game.season, def_team)
                # Fumbler/fumble_recoverer: ambiguous, use None
                for field in ("fumbler", "fumble_recoverer"):
                    name = getattr(play, field, None)
                    if name:
                        self.get_or_create(name, game.season, None)

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

    @staticmethod
    def _non_possession_team(game, play) -> str | None:
        """Determine team abbreviation for the non-possession (defensive) team."""
        tid = play.possession_team_id
        if tid is None:
            return None
        if tid == game.home_team_id:
            return game.away_team
        if tid == game.away_team_id:
            return game.home_team
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
