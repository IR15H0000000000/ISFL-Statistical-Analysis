"""Tests for the player registry."""

from pathlib import Path
from unittest.mock import patch

from isfl_epa.parser.schema import Game, ParsedPlay, PlayType
from isfl_epa.players.registry import PlayerRegistry


class TestBasicOperations:
    def test_create_new_player(self):
        reg = PlayerRegistry()
        pid = reg.get_or_create("Smith, J.", season=50, team="NYS")
        assert pid == 1
        assert reg.player_count == 1

    def test_same_name_returns_same_id(self):
        reg = PlayerRegistry()
        p1 = reg.get_or_create("Smith, J.", season=50, team="NYS")
        p2 = reg.get_or_create("Smith, J.", season=50, team="NYS")
        assert p1 == p2
        assert reg.player_count == 1

    def test_different_names_get_different_ids(self):
        reg = PlayerRegistry()
        p1 = reg.get_or_create("Smith, J.", season=50, team="NYS")
        p2 = reg.get_or_create("Jones, K.", season=50, team="BAL")
        assert p1 != p2
        assert reg.player_count == 2

    def test_get_player(self):
        reg = PlayerRegistry()
        pid = reg.get_or_create("Smith, J.", season=50, team="NYS")
        info = reg.get_player(pid)
        assert info["canonical_name"] == "Smith, J."
        assert info["first_seen_season"] == 50
        assert info["last_seen_season"] == 50

    def test_get_player_id(self):
        reg = PlayerRegistry()
        pid = reg.get_or_create("Smith, J.", season=50, team="NYS")
        assert reg.get_player_id("Smith, J.") == pid
        assert reg.get_player_id("Unknown, X.") is None


class TestCrossSeasonLinking:
    def test_same_name_across_seasons(self):
        reg = PlayerRegistry()
        p1 = reg.get_or_create("Smith, J.", season=45, team="NYS")
        p2 = reg.get_or_create("Smith, J.", season=50, team="NYS")
        assert p1 == p2
        info = reg.get_player(p1)
        assert info["first_seen_season"] == 45
        assert info["last_seen_season"] == 50

    def test_case_insensitive_matching(self):
        reg = PlayerRegistry()
        p1 = reg.get_or_create("Smith, J.", season=50, team="NYS")
        p2 = reg.get_or_create("smith, j.", season=51, team="NYS")
        assert p1 == p2

    def test_whitespace_normalization(self):
        reg = PlayerRegistry()
        p1 = reg.get_or_create("Smith, J.", season=50, team="NYS")
        p2 = reg.get_or_create(" Smith, J. ", season=51, team="NYS")
        assert p1 == p2

    def test_aliases_tracked(self):
        reg = PlayerRegistry()
        pid = reg.get_or_create("Smith, J.", season=45, team="NYS")
        reg.get_or_create("Smith, J.", season=50, team="BAL")
        aliases = reg.get_aliases(pid)
        assert len(aliases) == 2
        assert aliases[0]["season"] == 45
        assert aliases[1]["season"] == 50


class TestOverrides:
    def test_override_links_alias(self, tmp_path):
        override_file = tmp_path / "overrides.yaml"
        override_file.write_text(
            "merge:\n"
            "  - canonical: \"Smith, J.\"\n"
            "    aliases: [\"J. Smith\"]\n"
        )
        with patch("isfl_epa.players.registry._OVERRIDES_PATH", override_file):
            reg = PlayerRegistry()
            p1 = reg.get_or_create("Smith, J.", season=45, team="NYS")
            p2 = reg.get_or_create("J. Smith", season=50, team="NYS")
            assert p1 == p2

    def test_override_file_missing(self, tmp_path):
        missing = tmp_path / "nonexistent.yaml"
        with patch("isfl_epa.players.registry._OVERRIDES_PATH", missing):
            reg = PlayerRegistry()
            # Should work fine without overrides
            pid = reg.get_or_create("Smith, J.", season=50, team="NYS")
            assert pid == 1


class TestMerge:
    def test_merge_players(self):
        reg = PlayerRegistry()
        p1 = reg.get_or_create("Smith, J.", season=45, team="NYS")
        p2 = reg.get_or_create("Smith, John", season=50, team="NYS")
        assert p1 != p2

        reg.merge(keep_id=p1, remove_id=p2)
        assert reg.player_count == 1
        assert reg.get_player_id("Smith, John") == p1
        info = reg.get_player(p1)
        assert info["last_seen_season"] == 50

    def test_merge_nonexistent(self):
        reg = PlayerRegistry()
        pid = reg.get_or_create("Smith, J.", season=50, team="NYS")
        reg.merge(keep_id=pid, remove_id=999)  # should not crash
        assert reg.player_count == 1


class TestBuildFromGames:
    def test_registers_all_players(self):
        game = Game(
            id=1000, season=50, league="ISFL",
            home_team="HOM", away_team="AWY",
            home_team_id=1, away_team_id=2,
            plays=[
                ParsedPlay(
                    game_id=1000, quarter=1, clock="10:00",
                    play_type=PlayType.PASS,
                    description="Pass by QB, A., complete to WR, B. for 10 yds",
                    passer="QB, A.", receiver="WR, B.",
                    yards_gained=10, possession_team_id=1,
                ),
                ParsedPlay(
                    game_id=1000, quarter=1, clock="9:30",
                    play_type=PlayType.RUSH,
                    description="Rush by RB, C. for 5 yds. Tackled by LB, D.",
                    rusher="RB, C.", tackler="LB, D.",
                    yards_gained=5, possession_team_id=2,
                ),
            ],
        )
        reg = PlayerRegistry()
        reg.build_from_games([game])
        assert reg.player_count == 4
        assert reg.get_player_id("QB, A.") is not None
        assert reg.get_player_id("WR, B.") is not None
        assert reg.get_player_id("RB, C.") is not None
        assert reg.get_player_id("LB, D.") is not None
