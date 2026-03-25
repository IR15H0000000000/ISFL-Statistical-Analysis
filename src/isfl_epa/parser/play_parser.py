"""Parse raw PBP play dicts into structured ParsedPlay objects.

Works with both Format A (S27+ JSON) and Format B (S1-26 HTML, after
normalization by pbp_html.py). The play description text patterns are
shared across both formats.
"""

import logging
import re

from isfl_epa.parser.schema import Game, ParsedPlay, PlayType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Structured field parsers (t, o, s)
# ---------------------------------------------------------------------------

_DOWN_DIST_RE = re.compile(r"(\d)(?:st|nd|rd|th) and (\d+|inches|Goal)")
_FIELD_POS_RE = re.compile(r"(\w+) - (\d+)")
_SCORE_RE = re.compile(r"(\w+) (\d+) - (\w+) (\d+)")


def parse_down_distance(t_field: str) -> tuple[int | None, int | None, str | None]:
    """Parse '2nd and 7' → (2, 7, '7'). Returns (None, None, None) for markers."""
    if not t_field or t_field == "---":
        return None, None, None
    m = _DOWN_DIST_RE.match(t_field)
    if not m:
        return None, None, None
    down = int(m.group(1))
    dist_text = m.group(2)
    if dist_text == "inches":
        return down, None, "inches"
    if dist_text == "Goal":
        return down, None, "Goal"
    return down, int(dist_text), dist_text


def parse_field_position(o_field: str) -> tuple[str | None, int | None]:
    """Parse 'SJS - 25' → ('SJS', 25). Returns (None, None) for markers."""
    if not o_field or o_field == "--":
        return None, None
    m = _FIELD_POS_RE.match(o_field)
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


def parse_score(s_field: str | None) -> tuple[str | None, int | None, str | None, int | None]:
    """Parse 'NYS 3 - SJS 0' → ('NYS', 3, 'SJS', 0)."""
    if not s_field:
        return None, None, None, None
    m = _SCORE_RE.match(s_field)
    if not m:
        return None, None, None, None
    return m.group(1), int(m.group(2)), m.group(3), int(m.group(4))


# ---------------------------------------------------------------------------
# Description (m field) regex patterns
# ---------------------------------------------------------------------------

# Primary action patterns — checked in order, first match wins
_QUARTER_MARKER_RE = re.compile(
    r"^\d+:\d+ - .*(Quarter|Half|Overtime|OVERTIME)|End of Game|00:00"
)
_KICKOFF_TOUCHBACK_RE = re.compile(
    r"Kickoff by (.+?) (?:through the back|deep into the endzone)"
)
_KICKOFF_RETURN_RE = re.compile(
    r"Kickoff of (\d+) yards.*?Returned by (.+?) for (\d+) yards"
)
_KICKOFF_SIMPLE_RE = re.compile(r"(.+?) kicks off")
_KICKOFF_OF_YARDS_RE = re.compile(r"Kickoff by (.+?) of (\d+) yards")
_KICKOFF_YARD_RETURN_RE = re.compile(r"A (\d+) yard return")
_ONSIDE_KICKOFF_RE = re.compile(
    r"Onsides? Kickoff by (.+?) of (\d+) yards"
)
_ONSIDE_RECOVERY_RE = re.compile(
    r"Onsides? Kickoff by (.+?)\. Recovered by"
)
_FREE_KICK_RE = re.compile(r"Free Kick by (.+?) of (\d+) yards")
_PUNT_RE = re.compile(r"Punt by (.+?) of (\d+) yards")
_PUNT_BLOCKED_RE = re.compile(r"Punt by (.+?) is BLOCKED BY ([^<]+)\.(?=\.|<|\s|$)")
_FG_RE = re.compile(r"(\d+) yard FG by (.+?) is (good|NO good)")
_FG_BLOCKED_RE = re.compile(r"(\d+) yard FG by (.+?) is BLOCKED by ([^<]+)\.(?=\.|<|\s|$)")
_SACK_RE = re.compile(r"(.+?) (?:is )?SACKED by (.+?) - \w+ for (-?\d+) yds")
_PASS_COMPLETE_RE = re.compile(
    r"Pass by (.+?), complete to (.+?) for (-?\d+ yds|a short gain)"
)
_PASS_INCOMPLETE_BROKEN_RE = re.compile(
    r"Pass by (.+?) to (.+?) is incomplete\. Broken up by ([^<]+)\.(?=\.|<|\s|$)"
)
_PASS_INCOMPLETE_FALLS_RE = re.compile(
    r"Pass by (.+?) to (.+?) falls incomplete"
)
_PASS_INCOMPLETE_DROPPED_RE = re.compile(
    r"Pass by (.+?) to (.+?) was dropped"
)
_PASS_INTERCEPTED_RE = re.compile(
    r"Pass by (.+?),? to (.+?)\.\."
)
_RUSH_RE = re.compile(r"Rush by (.+?) for (-?\d+ yds|a short gain)")
_KNEEL_RE = re.compile(r"Offense kneels")
_SPIKE_RE = re.compile(r"spikes the ball")
_PENALTY_NULLIFY_RE = re.compile(
    r"(?:Pass |Rush )?Play nullified by (.+?) Penalty on (.+?): (.+?)(?:\.|<)"
)
_PENALTY_STANDALONE_RE = re.compile(
    r"^(.+?) Penalty on (.+?): (.+?)(?:\.|$)"
)
_TIMEOUT_STANDALONE_RE = re.compile(r"^Timeout called by (\w+)")
_TIMEOUT_OLD_FORMAT_RE = re.compile(r"^(.+?)\s*:\s*Timeout")
_THROWAWAY_RE = re.compile(r"(.+?) throws the ball away")
_TURNOVER_ON_DOWNS_STANDALONE_RE = re.compile(r"^Turnover on downs")

# Overlay patterns — checked on every play after primary
_TOUCHDOWN_RE = re.compile(r"TOUCHDOWN")
_PAT_RE = re.compile(r"\((.+?) kick (good|no good)\)", re.IGNORECASE)
_FIRST_DOWN_RE = re.compile(r"First Down")
_FUMBLE_LOST_RE = re.compile(
    r"FUMBLE recovered by (.+?) at .+? yard line and returned for (\d+)"
)
_FUMBLE_KEPT_RE = re.compile(r"FUMBLE by (.+?), recovered by ([^<]+)\.(?=\.|<|\s|$)")
_INTERCEPTION_RE = re.compile(
    r"INTERCEPTION by (.+?) at .+? yard line and returned for (-?\d+)"
)
_TIMEOUT_APPENDED_RE = re.compile(r"Timeout called by (\w+)")
_TURNOVER_ON_DOWNS_RE = re.compile(r"Turnover on downs")
_AUTO_FIRST_DOWN_RE = re.compile(r"Automatic First Down")
_SAFETY_RE = re.compile(r"Safety")
_TACKLE_RE = re.compile(r"Tackle by ([^<]+)\.(?=\.|<|\s|$)")
_PUNT_RETURN_RE = re.compile(r"Returned by (.+?) for (\d+) yards")


def _parse_description(desc: str) -> dict:
    """Parse the m field into a dict of extracted fields."""
    result: dict = {}

    # --- Primary action ---
    if _QUARTER_MARKER_RE.search(desc):
        result["play_type"] = PlayType.QUARTER_MARKER
        return result

    if m := _KICKOFF_TOUCHBACK_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kicker"] = m.group(1)
        # Check for merged kickoff+TD (rare: kickoff return TD in HTML format)
        if _TOUCHDOWN_RE.search(desc):
            result["touchdown"] = True
        return result

    if m := _KICKOFF_RETURN_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kick_yards"] = int(m.group(1))
        result["returner"] = m.group(2)
        result["yards_gained"] = int(m.group(3))
        return result

    if m := _ONSIDE_RECOVERY_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kicker"] = m.group(1)
        return result

    if m := _ONSIDE_KICKOFF_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kicker"] = m.group(1)
        result["kick_yards"] = int(m.group(2))
        if rm := _PUNT_RETURN_RE.search(desc):
            result["returner"] = rm.group(1)
            result["yards_gained"] = int(rm.group(2))
        return result

    if m := _FREE_KICK_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kicker"] = m.group(1)
        result["kick_yards"] = int(m.group(2))
        if rm := _PUNT_RETURN_RE.search(desc):
            result["returner"] = rm.group(1)
            result["yards_gained"] = int(rm.group(2))
        return result

    if m := _KICKOFF_OF_YARDS_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kicker"] = m.group(1)
        result["kick_yards"] = int(m.group(2))
        # Narrative return (HTML merged rows): "A 92 yard return"
        if rm := _KICKOFF_YARD_RETURN_RE.search(desc):
            result["yards_gained"] = int(rm.group(1))
        elif rm := _PUNT_RETURN_RE.search(desc):
            result["returner"] = rm.group(1)
            result["yards_gained"] = int(rm.group(2))
        return result

    if m := _KICKOFF_SIMPLE_RE.search(desc):
        result["play_type"] = PlayType.KICKOFF
        result["kicker"] = m.group(1)
        return result

    if m := _PUNT_RE.search(desc):
        result["play_type"] = PlayType.PUNT
        result["kicker"] = m.group(1)
        result["kick_yards"] = int(m.group(2))
        if pm := _PUNT_RETURN_RE.search(desc):
            result["returner"] = pm.group(1)
            result["yards_gained"] = int(pm.group(2))
        return result

    if m := _PUNT_BLOCKED_RE.search(desc):
        result["play_type"] = PlayType.PUNT
        result["kicker"] = m.group(1)
        if pm := _PUNT_RETURN_RE.search(desc):
            result["returner"] = pm.group(1)
            result["yards_gained"] = int(pm.group(2))
        return result

    if m := _FG_RE.search(desc):
        result["play_type"] = PlayType.FIELD_GOAL
        result["fg_distance"] = int(m.group(1))
        result["kicker"] = m.group(2)
        result["fg_good"] = m.group(3) == "good"
        return result

    if m := _FG_BLOCKED_RE.search(desc):
        result["play_type"] = PlayType.FIELD_GOAL
        result["fg_distance"] = int(m.group(1))
        result["kicker"] = m.group(2)
        result["fg_good"] = False
        return result

    if m := _SACK_RE.search(desc):
        result["play_type"] = PlayType.SACK
        result["passer"] = m.group(1)
        result["sacker"] = m.group(2)
        result["yards_gained"] = int(m.group(3))
        return result

    if m := _PASS_COMPLETE_RE.search(desc):
        result["play_type"] = PlayType.PASS
        result["passer"] = m.group(1)
        result["receiver"] = m.group(2)
        yards_str = m.group(3)
        if yards_str == "a short gain":
            result["yards_gained"] = 0
        else:
            result["yards_gained"] = int(yards_str.replace(" yds", ""))
        if tm := _TACKLE_RE.search(desc):
            result["tackler"] = tm.group(1)
        return result

    if m := _PASS_INCOMPLETE_BROKEN_RE.search(desc):
        result["play_type"] = PlayType.PASS
        result["passer"] = m.group(1)
        result["receiver"] = m.group(2)
        result["tackler"] = m.group(3)  # defender who broke it up
        result["yards_gained"] = 0
        return result

    if m := _PASS_INCOMPLETE_FALLS_RE.search(desc):
        result["play_type"] = PlayType.PASS
        result["passer"] = m.group(1)
        result["receiver"] = m.group(2)
        result["yards_gained"] = 0
        return result

    if m := _PASS_INCOMPLETE_DROPPED_RE.search(desc):
        result["play_type"] = PlayType.PASS
        result["passer"] = m.group(1)
        result["receiver"] = m.group(2)
        result["yards_gained"] = 0
        return result

    if m := _PASS_INTERCEPTED_RE.search(desc):
        if _INTERCEPTION_RE.search(desc):
            result["play_type"] = PlayType.PASS
            result["passer"] = m.group(1)
            result["receiver"] = m.group(2)
            result["yards_gained"] = 0
            return result

    if m := _RUSH_RE.search(desc):
        result["play_type"] = PlayType.RUSH
        result["rusher"] = m.group(1)
        yards_str = m.group(2)
        if yards_str == "a short gain":
            result["yards_gained"] = 0
        else:
            result["yards_gained"] = int(yards_str.replace(" yds", ""))
        if tm := _TACKLE_RE.search(desc):
            result["tackler"] = tm.group(1)
        return result

    if _KNEEL_RE.search(desc):
        result["play_type"] = PlayType.KNEEL
        return result

    if _SPIKE_RE.search(desc):
        result["play_type"] = PlayType.SPIKE
        return result

    if m := _PENALTY_NULLIFY_RE.search(desc):
        result["play_type"] = PlayType.PENALTY
        result["penalty"] = True
        result["penalty_team"] = m.group(1)
        # Player name is in group 2, penalty type in group 3
        result["penalty_type"] = m.group(3).strip()
        return result

    if m := _PENALTY_STANDALONE_RE.search(desc):
        result["play_type"] = PlayType.PENALTY
        result["penalty"] = True
        result["penalty_team"] = m.group(1)
        result["penalty_type"] = m.group(3).strip()
        return result

    if m := _TIMEOUT_STANDALONE_RE.search(desc):
        result["play_type"] = PlayType.TIMEOUT
        return result

    if _TIMEOUT_OLD_FORMAT_RE.search(desc):
        result["play_type"] = PlayType.TIMEOUT
        return result

    if m := _THROWAWAY_RE.search(desc):
        result["play_type"] = PlayType.PASS
        result["passer"] = m.group(1)
        result["yards_gained"] = 0
        return result

    if _TURNOVER_ON_DOWNS_STANDALONE_RE.search(desc):
        result["play_type"] = PlayType.PASS
        result["turnover_on_downs"] = True
        result["yards_gained"] = 0
        return result

    # Fallback: check for merged HTML plays that start with the kickoff narrative
    if "TOUCHDOWN" in desc and ("kicks off" in desc or "yard return" in desc):
        result["play_type"] = PlayType.KICKOFF
        result["touchdown"] = True
        return result

    result["play_type"] = PlayType.UNKNOWN
    return result


def _apply_overlays(desc: str, result: dict) -> None:
    """Apply overlay patterns that can appear on any play type."""
    if _TOUCHDOWN_RE.search(desc):
        result["touchdown"] = True

    if m := _PAT_RE.search(desc):
        result["pat_good"] = m.group(2).lower() == "good"

    if _FIRST_DOWN_RE.search(desc) or _AUTO_FIRST_DOWN_RE.search(desc):
        result["first_down"] = True

    if _AUTO_FIRST_DOWN_RE.search(desc):
        result["penalty_auto_first"] = True

    if m := _FUMBLE_LOST_RE.search(desc):
        result["fumble"] = True
        result["fumble_lost"] = True
        result["fumble_recoverer"] = m.group(1)

    if m := _FUMBLE_KEPT_RE.search(desc):
        result["fumble"] = True
        result["fumbler"] = m.group(1)
        result["fumble_recoverer"] = m.group(2)

    if m := _INTERCEPTION_RE.search(desc):
        result["interception"] = True
        result["interceptor"] = m.group(1)

    if _TURNOVER_ON_DOWNS_RE.search(desc):
        result["turnover_on_downs"] = True

    if _SAFETY_RE.search(desc):
        result["safety"] = True


# ---------------------------------------------------------------------------
# Top-level parsers
# ---------------------------------------------------------------------------

_QUARTER_KEYS = [("Q1", 1), ("Q2", 2), ("Q3", 3), ("Q4", 4), ("OT", 5)]


def parse_play(raw_play: dict, quarter: int, game_id: int) -> ParsedPlay:
    """Parse a single raw play dict into a ParsedPlay."""
    down, distance, distance_text = parse_down_distance(raw_play.get("t", ""))
    yl_team, yl = parse_field_position(raw_play.get("o", ""))
    if distance is None and distance_text == "Goal" and yl is not None:
        distance = yl
    elif distance is None and distance_text == "inches":
        distance = 1
    away_team, score_away, home_team, score_home = parse_score(raw_play.get("s"))

    desc = raw_play.get("m", "")
    parsed = _parse_description(desc)
    _apply_overlays(desc, parsed)

    play_type = parsed.pop("play_type", PlayType.UNKNOWN)

    return ParsedPlay(
        game_id=game_id,
        quarter=quarter,
        clock=raw_play.get("c", ""),
        score_away=score_away,
        score_home=score_home,
        away_team=away_team,
        home_team=home_team,
        possession_team_id=raw_play.get("id"),
        down=down,
        distance=distance,
        distance_text=distance_text,
        yard_line=yl,
        yard_line_team=yl_team,
        play_type=play_type,
        description=desc,
        css=raw_play.get("css", ""),
        **parsed,
    )


def parse_game(raw_game: dict, season: int, league: str) -> Game:
    """Parse an entire raw game dict into a Game with ParsedPlays."""
    game_id = raw_game["id"]
    plays: list[ParsedPlay] = []
    unparsed: list[dict] = []

    for q_key, q_num in _QUARTER_KEYS:
        for raw_play in raw_game.get(q_key, []):
            play = parse_play(raw_play, q_num, game_id)
            plays.append(play)
            if play.play_type == PlayType.UNKNOWN:
                unparsed.append({
                    "quarter": q_num,
                    "clock": play.clock,
                    "description": play.description,
                })

    # Derive home/away from first play with score data
    home_team = away_team = None
    home_team_id = away_team_id = None
    for p in plays:
        if p.away_team and p.home_team:
            away_team = p.away_team
            home_team = p.home_team
            break

    # Derive team IDs by correlating possession_team_id with team abbreviations.
    # Strategy: find a kickoff followed by a scrimmage play — the scrimmage play's
    # possession_team_id is the receiving team, and the yard_line_team on that play
    # at the 25 (touchback) tells us which team that id belongs to.
    team_ids = {p.possession_team_id for p in plays if p.possession_team_id is not None}
    id_to_abbr: dict[int, str] = {}
    if len(team_ids) == 2 and away_team and home_team:
        known_abbrs = {away_team, home_team}
        prev_was_kickoff = False
        for p in plays:
            if p.play_type == PlayType.KICKOFF:
                prev_was_kickoff = True
                continue
            if (prev_was_kickoff
                    and p.possession_team_id is not None
                    and p.yard_line_team in known_abbrs
                    and p.yard_line == 25):
                # After a touchback, the receiving team starts at their own 25
                # yard_line_team == receiving team == possession team
                id_to_abbr[p.possession_team_id] = p.yard_line_team
                if len(id_to_abbr) == 2:
                    break
            prev_was_kickoff = False

        # Broader: first scrimmage play after any kickoff — the possession team
        # is the receiving team, at their own yard line (return spot)
        if len(id_to_abbr) < 2:
            prev_was_kickoff = False
            for p in plays:
                if p.play_type == PlayType.KICKOFF:
                    prev_was_kickoff = True
                    continue
                if (prev_was_kickoff
                        and p.possession_team_id is not None
                        and p.possession_team_id not in id_to_abbr
                        and p.yard_line_team in known_abbrs
                        and p.yard_line is not None
                        and p.yard_line <= 45
                        and p.play_type in (PlayType.PASS, PlayType.RUSH, PlayType.SACK)):
                    id_to_abbr[p.possession_team_id] = p.yard_line_team
                    if len(id_to_abbr) == 2:
                        break
                prev_was_kickoff = False

        if len(id_to_abbr) == 2:
            for tid, abbr in id_to_abbr.items():
                if abbr == away_team:
                    away_team_id = tid
                elif abbr == home_team:
                    home_team_id = tid
        elif len(id_to_abbr) == 1:
            known_id, known_abbr = next(iter(id_to_abbr.items()))
            other_id = (team_ids - {known_id}).pop()
            if known_abbr == away_team:
                away_team_id = known_id
                home_team_id = other_id
            else:
                home_team_id = known_id
                away_team_id = other_id
        else:
            # Fallback: sorted IDs (old behavior)
            id_list = sorted(team_ids)
            away_team_id, home_team_id = id_list[0], id_list[1]

    total = len(plays)
    parsed_count = total - len(unparsed)
    if unparsed:
        logger.warning(
            "Game %d: Parsed %d/%d plays (%.1f%%). %d unparsed.",
            game_id, parsed_count, total, 100 * parsed_count / total if total else 0,
            len(unparsed),
        )
        for u in unparsed[:5]:
            logger.debug("  Unparsed: Q%d %s: %s", u["quarter"], u["clock"], u["description"][:100])

    return Game(
        id=game_id,
        season=season,
        league=league,
        home_team=home_team,
        away_team=away_team,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        plays=plays,
    )
