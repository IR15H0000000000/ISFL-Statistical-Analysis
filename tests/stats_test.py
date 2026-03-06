"""Unit tests for stats aggregation using synthetic game data."""

from isfl_epa.parser.schema import Game, ParsedPlay, PlayType
from isfl_epa.stats.aggregation import (
    game_player_defensive,
    game_player_passing,
    game_player_receiving,
    game_player_rushing,
    game_team_stats,
)


def _make_game(plays: list[ParsedPlay], season: int = 50) -> Game:
    """Create a Game with the given plays and default team setup."""
    return Game(
        id=1000,
        season=season,
        league="ISFL",
        home_team="HOM",
        away_team="AWY",
        home_team_id=1,
        away_team_id=2,
        plays=plays,
    )


def _play(play_type: PlayType, description: str, **kwargs) -> ParsedPlay:
    """Shorthand for creating a ParsedPlay."""
    return ParsedPlay(
        game_id=1000, quarter=1, clock="10:00",
        play_type=play_type, description=description,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Passing
# ---------------------------------------------------------------------------


class TestPlayerPassing:
    def test_complete_pass(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 15 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=15,
                  possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert len(stats) == 1
        assert stats[0].player == "QB, A."
        assert stats[0].comp == 1
        assert stats[0].att == 1
        assert stats[0].yards == 15

    def test_incomplete_pass(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A. to WR, B. falls incomplete",
                  passer="QB, A.", receiver="WR, B.", yards_gained=0,
                  possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert stats[0].comp == 0
        assert stats[0].att == 1
        assert stats[0].yards == 0

    def test_pass_td(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 30 yds TOUCHDOWN",
                  passer="QB, A.", receiver="WR, B.", yards_gained=30,
                  touchdown=True, possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert stats[0].td == 1

    def test_interception(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., to WR, B...INTERCEPTED by DB, C.",
                  passer="QB, A.", receiver="WR, B.", interception=True,
                  interceptor="DB, C.", possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert stats[0].interceptions == 1
        assert stats[0].att == 1

    def test_sack_credited_to_passer(self):
        game = _make_game([
            _play(PlayType.SACK, "QB, A. sacked by DE, X. for -8 yds",
                  passer="QB, A.", sacker="DE, X.", yards_gained=-8,
                  possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert stats[0].sacks == 1
        assert stats[0].sack_yards == -8
        assert stats[0].att == 0  # sacks are NOT pass attempts

    def test_two_point_excluded(self):
        game = _make_game([
            _play(PlayType.PASS, "2 point conversion Pass by QB, A., complete to WR, B. for 2 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=2,
                  possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert len(stats) == 0

    def test_multiple_passers(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB1, A., complete to WR, B. for 10 yds",
                  passer="QB1, A.", receiver="WR, B.", yards_gained=10,
                  possession_team_id=1),
            _play(PlayType.PASS, "Pass by QB2, C., complete to WR, D. for 20 yds",
                  passer="QB2, C.", receiver="WR, D.", yards_gained=20,
                  possession_team_id=2),
        ])
        stats = game_player_passing(game)
        assert len(stats) == 2
        by_name = {s.player: s for s in stats}
        assert by_name["QB1, A."].yards == 10
        assert by_name["QB2, C."].yards == 20

    def test_team_attribution(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 10 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=10,
                  possession_team_id=1),
        ])
        stats = game_player_passing(game)
        assert stats[0].team == "HOM"


# ---------------------------------------------------------------------------
# Rushing
# ---------------------------------------------------------------------------


class TestPlayerRushing:
    def test_basic_rush(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush by RB, A. for 5 yds",
                  rusher="RB, A.", yards_gained=5, possession_team_id=2),
        ])
        stats = game_player_rushing(game)
        assert stats[0].att == 1
        assert stats[0].yards == 5
        assert stats[0].team == "AWY"

    def test_rush_td(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush by RB, A. for 1 yds TOUCHDOWN",
                  rusher="RB, A.", yards_gained=1, touchdown=True,
                  possession_team_id=1),
        ])
        stats = game_player_rushing(game)
        assert stats[0].td == 1

    def test_fumble_lost(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush by RB, A. for 3 yds fumble",
                  rusher="RB, A.", yards_gained=3, fumble=True,
                  fumble_lost=True, possession_team_id=1),
        ])
        stats = game_player_rushing(game)
        assert stats[0].fumbles == 1

    def test_two_point_excluded(self):
        game = _make_game([
            _play(PlayType.RUSH, "2 point conversion Rush by RB, A. for 2 yds",
                  rusher="RB, A.", yards_gained=2, possession_team_id=1),
        ])
        stats = game_player_rushing(game)
        assert len(stats) == 0


# ---------------------------------------------------------------------------
# Receiving
# ---------------------------------------------------------------------------


class TestPlayerReceiving:
    def test_reception(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 15 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=15,
                  possession_team_id=1),
        ])
        stats = game_player_receiving(game)
        assert len(stats) == 1
        assert stats[0].player == "WR, B."
        assert stats[0].receptions == 1
        assert stats[0].yards == 15

    def test_incomplete_not_counted(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A. to WR, B. falls incomplete",
                  passer="QB, A.", receiver="WR, B.", yards_gained=0,
                  possession_team_id=1),
        ])
        stats = game_player_receiving(game)
        assert len(stats) == 0

    def test_receiving_td(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 30 yds TOUCHDOWN",
                  passer="QB, A.", receiver="WR, B.", yards_gained=30,
                  touchdown=True, possession_team_id=1),
        ])
        stats = game_player_receiving(game)
        assert stats[0].td == 1


# ---------------------------------------------------------------------------
# Defensive
# ---------------------------------------------------------------------------


class TestPlayerDefensive:
    def test_tackle(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush by RB, A. for 5 yds. Tackled by LB, X.",
                  rusher="RB, A.", tackler="LB, X.", yards_gained=5,
                  possession_team_id=1),
        ])
        stats = game_player_defensive(game)
        by_name = {s.player: s for s in stats}
        assert by_name["LB, X."].tackles == 1

    def test_sack(self):
        game = _make_game([
            _play(PlayType.SACK, "QB, A. sacked by DE, Y. for -8 yds",
                  passer="QB, A.", sacker="DE, Y.", yards_gained=-8,
                  possession_team_id=1),
        ])
        stats = game_player_defensive(game)
        by_name = {s.player: s for s in stats}
        assert by_name["DE, Y."].sacks == 1.0

    def test_interception(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., to WR, B...INTERCEPTED by DB, Z.",
                  passer="QB, A.", interceptor="DB, Z.", interception=True,
                  possession_team_id=1),
        ])
        stats = game_player_defensive(game)
        by_name = {s.player: s for s in stats}
        assert by_name["DB, Z."].interceptions == 1

    def test_fumble_recovery(self):
        game = _make_game([
            _play(PlayType.RUSH, "Rush by RB, A. for 3 yds fumble recovered by LB, W.",
                  rusher="RB, A.", fumble=True, fumble_lost=True,
                  fumble_recoverer="LB, W.", yards_gained=3,
                  possession_team_id=1),
        ])
        stats = game_player_defensive(game)
        by_name = {s.player: s for s in stats}
        assert by_name["LB, W."].fumble_recoveries == 1


# ---------------------------------------------------------------------------
# Team stats
# ---------------------------------------------------------------------------


class TestTeamStats:
    def test_basic_team_stats(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 10 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=10,
                  possession_team_id=1),
            _play(PlayType.RUSH, "Rush by RB, C. for 5 yds",
                  rusher="RB, C.", yards_gained=5,
                  possession_team_id=2),
        ])
        stats = game_team_stats(game)
        assert len(stats) == 2
        by_team = {s.team: s for s in stats}

        home = by_team["HOM"]
        assert home.pass_comp == 1
        assert home.pass_att == 1
        assert home.pass_yards == 10
        assert home.is_home is True

        away = by_team["AWY"]
        assert away.rush_att == 1
        assert away.rush_yards == 5
        assert away.is_home is False

    def test_spike_counts_as_team_att(self):
        game = _make_game([
            _play(PlayType.SPIKE, "Offense spikes the ball",
                  possession_team_id=1),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].pass_att == 1

    def test_kneels_s28_counted(self):
        """S28+: kneels count as rush att with -2 yards each."""
        game = _make_game([
            _play(PlayType.KNEEL, "Offense kneels", possession_team_id=1),
            _play(PlayType.KNEEL, "Offense kneels", possession_team_id=1),
        ], season=28)
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].rush_att == 2
        assert by_team["HOM"].rush_yards == -4

    def test_kneels_s27_not_counted(self):
        """S27: kneels NOT counted as rush attempts."""
        game = _make_game([
            _play(PlayType.KNEEL, "Offense kneels", possession_team_id=1),
        ], season=27)
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].rush_att == 0
        assert by_team["HOM"].rush_yards == 0

    def test_two_point_excluded_from_team(self):
        game = _make_game([
            _play(PlayType.PASS, "2 point conversion Pass by QB, A., complete to WR, B. for 2 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=2,
                  possession_team_id=1),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].pass_att == 0
        assert by_team["HOM"].pass_yards == 0

    def test_sack_attribution(self):
        game = _make_game([
            _play(PlayType.SACK, "QB, A. sacked by DE, X. for -8 yds",
                  passer="QB, A.", sacker="DE, X.", yards_gained=-8,
                  possession_team_id=1),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].sacks_taken == 1
        assert by_team["AWY"].sacks_made == 1

    def test_third_down_tracking(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 10 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=10,
                  first_down=True, down=3, distance=7,
                  possession_team_id=1),
            _play(PlayType.RUSH, "Rush by RB, C. for 2 yds",
                  rusher="RB, C.", yards_gained=2,
                  down=3, distance=5,
                  possession_team_id=1),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].third_down_att == 2
        assert by_team["HOM"].third_down_conv == 1  # only the first was a conversion

    def test_turnover_counting(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., to WR, B...INTERCEPTED by DB, Z.",
                  passer="QB, A.", interception=True, interceptor="DB, Z.",
                  possession_team_id=1),
            _play(PlayType.RUSH, "Rush by RB, C. for 3 yds fumble",
                  rusher="RB, C.", fumble=True, fumble_lost=True,
                  yards_gained=3, possession_team_id=2),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].interceptions_thrown == 1
        assert by_team["HOM"].turnovers == 1
        assert by_team["AWY"].fumbles_lost == 1
        assert by_team["AWY"].turnovers == 1

    def test_total_yards(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 20 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=20,
                  possession_team_id=1),
            _play(PlayType.RUSH, "Rush by RB, C. for 10 yds",
                  rusher="RB, C.", yards_gained=10,
                  possession_team_id=1),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].total_yards == 30

    def test_score_extraction(self):
        game = _make_game([
            _play(PlayType.PASS, "Pass by QB, A., complete to WR, B. for 10 yds",
                  passer="QB, A.", receiver="WR, B.", yards_gained=10,
                  possession_team_id=1,
                  score_away=14, score_home=21,
                  away_team="AWY", home_team="HOM"),
        ])
        stats = game_team_stats(game)
        by_team = {s.team: s for s in stats}
        assert by_team["HOM"].points_for == 21
        assert by_team["HOM"].points_against == 14
        assert by_team["AWY"].points_for == 14
        assert by_team["AWY"].points_against == 21
