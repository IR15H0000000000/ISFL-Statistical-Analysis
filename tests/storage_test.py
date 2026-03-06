"""Tests for storage layer (Parquet + PostgreSQL).

PostgreSQL tests require a running database and are skipped if unavailable.
Parquet tests use tmp_path and always run.
"""

import pytest

from isfl_epa.parser.schema import Game, ParsedPlay, PlayType
from isfl_epa.players.registry import PlayerRegistry
from isfl_epa.storage.parquet import write_season_plays, read_season_plays


def _make_game(season=50) -> Game:
    return Game(
        id=1000, season=season, league="ISFL",
        home_team="HOM", away_team="AWY",
        home_team_id=1, away_team_id=2,
        plays=[
            ParsedPlay(
                game_id=1000, quarter=1, clock="10:00",
                play_type=PlayType.PASS,
                description="Pass by QB, A., complete to WR, B. for 15 yds",
                passer="QB, A.", receiver="WR, B.",
                yards_gained=15, possession_team_id=1,
            ),
            ParsedPlay(
                game_id=1000, quarter=1, clock="9:30",
                play_type=PlayType.RUSH,
                description="Rush by RB, C. for 5 yds",
                rusher="RB, C.", yards_gained=5,
                possession_team_id=2,
            ),
        ],
    )


class TestParquet:
    def test_write_and_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr("isfl_epa.storage.parquet.DATA_DIR", tmp_path)
        game = _make_game()
        path = write_season_plays([game], season=50, league="ISFL")
        assert path.exists()

        monkeypatch.setattr("isfl_epa.storage.parquet.DATA_DIR", tmp_path)
        df = read_season_plays(season=50, league="ISFL")
        assert len(df) == 2
        assert "game_id" in df.columns
        assert "play_type" in df.columns
        assert "season" in df.columns

    def test_player_ids_populated(self, tmp_path, monkeypatch):
        monkeypatch.setattr("isfl_epa.storage.parquet.DATA_DIR", tmp_path)
        game = _make_game()
        registry = PlayerRegistry()
        registry.build_from_games([game])

        write_season_plays([game], season=50, league="ISFL", registry=registry)
        df = read_season_plays(season=50, league="ISFL")

        # First play has passer and receiver
        pass_play = df[df["play_type"] == "pass"].iloc[0]
        assert pass_play["player_id_passer"] is not None
        assert pass_play["player_id_receiver"] is not None

        # Second play has rusher
        rush_play = df[df["play_type"] == "rush"].iloc[0]
        assert rush_play["player_id_rusher"] is not None

    def test_schema_columns(self, tmp_path, monkeypatch):
        monkeypatch.setattr("isfl_epa.storage.parquet.DATA_DIR", tmp_path)
        game = _make_game()
        write_season_plays([game], season=50, league="ISFL")
        df = read_season_plays(season=50, league="ISFL")

        expected_cols = {
            "game_id", "season", "league", "quarter", "clock",
            "play_type", "description", "yards_gained",
            "passer", "rusher", "receiver", "possession_team",
        }
        assert expected_cols.issubset(set(df.columns))


def _pg_available():
    """Check if PostgreSQL is available for testing."""
    try:
        from isfl_epa.storage.database import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(engine.dialect.do_ping(conn) if hasattr(engine.dialect, 'do_ping') else conn.exec_driver_sql("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")
class TestPostgreSQL:
    @pytest.fixture(autouse=True)
    def setup_db(self):
        from isfl_epa.storage.database import get_engine, create_tables, metadata
        self.engine = get_engine()
        create_tables(self.engine)
        yield
        # Clean up test data
        with self.engine.begin() as conn:
            for table in reversed(metadata.sorted_tables):
                conn.execute(table.delete())

    def test_load_and_query_plays(self):
        from isfl_epa.storage.database import load_season, query_plays
        game = _make_game()
        registry = PlayerRegistry()
        registry.build_from_games([game])

        load_season(self.engine, [game], registry)
        plays = query_plays(self.engine, game_id=1000)
        assert len(plays) == 2

    def test_query_by_player_id(self):
        from isfl_epa.storage.database import load_season, query_player_plays
        game = _make_game()
        registry = PlayerRegistry()
        registry.build_from_games([game])

        load_season(self.engine, [game], registry)
        pid = registry.get_player_id("QB, A.")
        plays = query_player_plays(self.engine, pid)
        assert len(plays) >= 1
        assert plays[0]["passer"] == "QB, A."
