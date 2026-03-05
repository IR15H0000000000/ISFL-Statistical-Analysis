"""Tests for the play parser using real play examples."""

from isfl_epa.parser.play_parser import (
    parse_down_distance,
    parse_field_position,
    parse_play,
    parse_score,
)
from isfl_epa.parser.schema import PlayType


# ---------------------------------------------------------------------------
# Structured field parsers
# ---------------------------------------------------------------------------

class TestParseDownDistance:
    def test_normal(self):
        assert parse_down_distance("1st and 10") == (1, 10, "10")

    def test_second_and_short(self):
        assert parse_down_distance("2nd and 3") == (2, 3, "3")

    def test_third_and_long(self):
        assert parse_down_distance("3rd and 15") == (3, 15, "15")

    def test_fourth_and_inches(self):
        assert parse_down_distance("4th and inches") == (4, None, "inches")

    def test_goal(self):
        assert parse_down_distance("1st and Goal") == (1, None, "Goal")

    def test_marker(self):
        assert parse_down_distance("---") == (None, None, None)

    def test_empty(self):
        assert parse_down_distance("") == (None, None, None)


class TestParseFieldPosition:
    def test_normal(self):
        assert parse_field_position("SJS - 25") == ("SJS", 25)

    def test_own_one(self):
        assert parse_field_position("COL - 1") == ("COL", 1)

    def test_midfield(self):
        assert parse_field_position("BAL - 50") == ("BAL", 50)

    def test_marker(self):
        assert parse_field_position("--") == (None, None)

    def test_empty(self):
        assert parse_field_position("") == (None, None)


class TestParseScore:
    def test_normal(self):
        assert parse_score("NYS 3 - SJS 0") == ("NYS", 3, "SJS", 0)

    def test_high_score(self):
        assert parse_score("NYS 24 - SJS 17") == ("NYS", 24, "SJS", 17)

    def test_none(self):
        assert parse_score(None) == (None, None, None, None)


# ---------------------------------------------------------------------------
# Play description parsing
# ---------------------------------------------------------------------------

def _make_raw(m: str, **kwargs) -> dict:
    """Create a raw play dict for testing."""
    raw = {
        "c": "12:00",
        "t": "1st and 10",
        "o": "SJS - 25",
        "s": "NYS 0 - SJS 0",
        "m": m,
        "css": "",
        "id": 6,
    }
    raw.update(kwargs)
    return raw


class TestPassPlays:
    def test_complete(self):
        p = parse_play(
            _make_raw("Pass by Patterson, J., complete to Dogwood, M. for 3 yds. Tackle by Passiveman, P.."),
            1, 9630,
        )
        assert p.play_type == PlayType.PASS
        assert p.passer == "Patterson, J."
        assert p.receiver == "Dogwood, M."
        assert p.yards_gained == 3
        assert p.tackler == "Passiveman, P."

    def test_complete_short_gain(self):
        p = parse_play(
            _make_raw("Pass by McDummy Jr., W., complete to Skywalker, A. for a short gain. Tackle by Strong, W.."),
            1, 100,
        )
        assert p.play_type == PlayType.PASS
        assert p.yards_gained == 0

    def test_incomplete_broken_up(self):
        p = parse_play(
            _make_raw("Pass by Dyson, E. to Feet-Lover Sr., Z. is incomplete. Broken up by Andre, B.."),
            1, 9630,
        )
        assert p.play_type == PlayType.PASS
        assert p.passer == "Dyson, E."
        assert p.receiver == "Feet-Lover Sr., Z."
        assert p.yards_gained == 0

    def test_incomplete_falls(self):
        p = parse_play(
            _make_raw("Pass by Wright, D. to Olsen, C. falls incomplete."),
            1, 4620,
        )
        assert p.play_type == PlayType.PASS
        assert p.yards_gained == 0

    def test_incomplete_dropped(self):
        p = parse_play(
            _make_raw("Pass by Wright, D. to Olsen, C. was dropped! Incomplete."),
            1, 4620,
        )
        assert p.play_type == PlayType.PASS
        assert p.yards_gained == 0

    def test_pass_with_td(self):
        p = parse_play(
            _make_raw("Pass by Dyson, E., complete to McDiddl, S. for 31 yds.<br/>TOUCHDOWN!<br/> (Wang kick good)"),
            1, 9630,
        )
        assert p.play_type == PlayType.PASS
        assert p.yards_gained == 31
        assert p.touchdown is True
        assert p.pat_good is True

    def test_pass_intercepted(self):
        p = parse_play(
            _make_raw(
                "Pass by Patterson, J., to Julius, O..<br/> INTERCEPTION by Passiveman, P. "
                "at the SJS - 42 yard line and returned for 14 yards.<br/>First Down!"
            ),
            1, 9630,
        )
        assert p.play_type == PlayType.PASS
        assert p.interception is True
        assert p.interceptor == "Passiveman, P."
        assert p.first_down is True

    def test_throwaway(self):
        p = parse_play(
            _make_raw("Bannings, L. throws the ball away to avoid the sack."),
            1, 100,
        )
        assert p.play_type == PlayType.PASS
        assert p.passer == "Bannings, L."
        assert p.yards_gained == 0


class TestRushPlays:
    def test_normal(self):
        p = parse_play(
            _make_raw("Rush by Dogwood, M. for 3 yds. Tackle by Passiveman, P.."),
            1, 9630,
        )
        assert p.play_type == PlayType.RUSH
        assert p.rusher == "Dogwood, M."
        assert p.yards_gained == 3
        assert p.tackler == "Passiveman, P."

    def test_short_gain(self):
        p = parse_play(
            _make_raw("Rush by Fisto, K. for a short gain. Tackle by Chanos, D.."),
            1, 9630,
        )
        assert p.play_type == PlayType.RUSH
        assert p.yards_gained == 0

    def test_negative_yards(self):
        p = parse_play(
            _make_raw("Rush by Dyson, E. for -1 yds. Tackle by Armstrong, A.."),
            1, 9630,
        )
        assert p.play_type == PlayType.RUSH
        assert p.yards_gained == -1

    def test_rush_with_fumble(self):
        p = parse_play(
            _make_raw(
                "Rush by Dyson, E. for -1 yds. Tackle by Armstrong, A..<br/> "
                "FUMBLE by Dyson, E., recovered by Dyson, E.."
            ),
            1, 9630,
        )
        assert p.play_type == PlayType.RUSH
        assert p.fumble is True

    def test_rush_td(self):
        p = parse_play(
            _make_raw("Rush by Hammerfall, R. for 3 yds.<br/>TOUCHDOWN!<br/> (Wang kick good)"),
            1, 9630,
        )
        assert p.play_type == PlayType.RUSH
        assert p.touchdown is True
        assert p.pat_good is True


class TestSack:
    def test_normal(self):
        p = parse_play(
            _make_raw("Patterson, J. is SACKED by Lionel Scrimmage - DT for -11 yds."),
            1, 9630,
        )
        assert p.play_type == PlayType.SACK
        assert p.passer == "Patterson, J."
        assert p.sacker == "Lionel Scrimmage"
        assert p.yards_gained == -11


class TestKickingPlays:
    def test_kickoff_touchback(self):
        p = parse_play(
            _make_raw("Kickoff by Wang, W. through the back of the endzone. Touchback.", t="---", o="--"),
            1, 9630,
        )
        assert p.play_type == PlayType.KICKOFF
        assert p.kicker == "Wang, W."

    def test_kickoff_return(self):
        p = parse_play(
            _make_raw("Kickoff of 62 yards. Returned by Dangle (C), Z. for 10 yards.<br/>First Down!"),
            1, 9630,
        )
        assert p.play_type == PlayType.KICKOFF
        assert p.kick_yards == 62
        assert p.returner == "Dangle (C), Z."
        assert p.yards_gained == 10

    def test_onside_kickoff(self):
        p = parse_play(
            _make_raw("Onsides Kickoff by Robinson (R), T. of 18 yards. Returned by Hawk, S. for 2 yards.<br/>First Down!"),
            1, 9630,
        )
        assert p.play_type == PlayType.KICKOFF
        assert p.kick_yards == 18

    def test_punt(self):
        p = parse_play(
            _make_raw("Punt by Robinson (R), T. of 33 yards.<br/> No return.<br/>First Down!"),
            1, 9630,
        )
        assert p.play_type == PlayType.PUNT
        assert p.kicker == "Robinson (R), T."
        assert p.kick_yards == 33

    def test_fg_good(self):
        p = parse_play(
            _make_raw("18 yard FG by Wang, W. is good."),
            1, 9630,
        )
        assert p.play_type == PlayType.FIELD_GOAL
        assert p.fg_distance == 18
        assert p.fg_good is True

    def test_fg_no_good(self):
        p = parse_play(
            _make_raw("47 yard FG by Jay-Jaymison, J. is NO good."),
            1, 9630,
        )
        assert p.play_type == PlayType.FIELD_GOAL
        assert p.fg_good is False


class TestPenalty:
    def test_nullifying(self):
        p = parse_play(
            _make_raw("Pass Play nullified by San Jose Penalty on Man, F.: Offensive Pass Interference."),
            1, 9630,
        )
        assert p.play_type == PlayType.PENALTY
        assert p.penalty is True
        assert p.penalty_type == "Offensive Pass Interference"

    def test_standalone(self):
        p = parse_play(
            _make_raw("SaberCats Penalty on Justice, T.: Delay of Game."),
            1, 4620,
        )
        assert p.play_type == PlayType.PENALTY
        assert p.penalty is True


class TestSpecialPlays:
    def test_kneel(self):
        p = parse_play(_make_raw("Offense kneels the ball."), 4, 9630)
        assert p.play_type == PlayType.KNEEL

    def test_quarter_marker(self):
        p = parse_play(
            _make_raw("15:00 - First Quarter", t="---", o="--"),
            1, 9630,
        )
        assert p.play_type == PlayType.QUARTER_MARKER

    def test_timeout(self):
        p = parse_play(
            _make_raw("Timeout called by SJS"),
            1, 9630,
        )
        assert p.play_type == PlayType.TIMEOUT

    def test_timeout_old_format(self):
        p = parse_play(
            _make_raw("San Jose SaberCats : Timeout"),
            1, 4620,
        )
        assert p.play_type == PlayType.TIMEOUT


class TestStructuredFields:
    def test_down_distance_parsed(self):
        p = parse_play(
            _make_raw("Rush by Dogwood, M. for 3 yds. Tackle by Passiveman, P.."),
            1, 9630,
        )
        assert p.down == 1
        assert p.distance == 10
        assert p.yard_line == 25
        assert p.yard_line_team == "SJS"
        assert p.score_away == 0
        assert p.score_home == 0
        assert p.away_team == "NYS"
        assert p.home_team == "SJS"

    def test_none_score_for_html(self):
        raw = _make_raw("Rush by Toriki (R), M. for 9 yds. Tackle by Gabagool, T..")
        raw["s"] = None  # HTML format has no score
        p = parse_play(raw, 1, 4620)
        assert p.score_away is None
        assert p.score_home is None


class TestBlockedPlays:
    def test_blocked_fg(self):
        p = parse_play(
            _make_raw("45 yard FG by Banana, S. is BLOCKED by Egghands, T.."),
            1, 9630,
        )
        assert p.play_type == PlayType.FIELD_GOAL
        assert p.fg_distance == 45
        assert p.kicker == "Banana, S."
        assert p.fg_good is False

    def test_blocked_punt_no_return(self):
        p = parse_play(
            _make_raw(
                "Punt by Bloomfield (R), L. is BLOCKED BY Jones, D.."
                "<br/> No return.<br/>First Down!"
            ),
            1, 9630,
        )
        assert p.play_type == PlayType.PUNT
        assert p.kicker == "Bloomfield (R), L."

    def test_blocked_punt_with_return(self):
        p = parse_play(
            _make_raw(
                "Punt by Powers, V. is BLOCKED BY Allen, O.."
                "<br/> Returned by Allen, O. for 19 yards.<br/>First Down!"
            ),
            1, 9630,
        )
        assert p.play_type == PlayType.PUNT
        assert p.kicker == "Powers, V."
        assert p.yards_gained == 19
