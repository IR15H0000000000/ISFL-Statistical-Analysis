"""Tests for FastAPI endpoints using SQLite in-memory database."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from isfl_epa.api.app import app
from isfl_epa.parser.schema import Game, ParsedPlay, PlayType
from isfl_epa.players.registry import PlayerRegistry
from isfl_epa.storage.database import (
    create_tables,
    load_registry,
    load_season,
    metadata,
    play_epa_table,
    player_season_epa_table,
    team_season_epa_table,
    plays_table,
)


def _make_game() -> Game:
    return Game(
        id=1000, season=50, league="ISFL",
        home_team="HOM", away_team="AWY",
        home_team_id=1, away_team_id=2,
        plays=[
            ParsedPlay(
                game_id=1000, quarter=1, clock="10:00",
                play_type=PlayType.PASS,
                description="Pass by QB, A., complete to WR, B. for 15 yds",
                passer="QB, A.", receiver="WR, B.",
                yards_gained=15, possession_team_id=1,
                score_away=0, score_home=0,
                away_team="AWY", home_team="HOM",
            ),
            ParsedPlay(
                game_id=1000, quarter=1, clock="9:30",
                play_type=PlayType.RUSH,
                description="Rush by RB, C. for 5 yds. Tackled by LB, D.",
                rusher="RB, C.", tackler="LB, D.",
                yards_gained=5, possession_team_id=2,
                score_away=0, score_home=0,
                away_team="AWY", home_team="HOM",
            ),
            ParsedPlay(
                game_id=1000, quarter=2, clock="5:00",
                play_type=PlayType.PASS,
                description="Pass by QB, A., complete to WR, B. for 30 yds TOUCHDOWN",
                passer="QB, A.", receiver="WR, B.",
                yards_gained=30, touchdown=True, possession_team_id=1,
                score_away=0, score_home=7,
                away_team="AWY", home_team="HOM",
            ),
        ],
    )


def _make_extra_games() -> list[Game]:
    """Extra games with different opponents — needed for intersection-based team_id resolution.

    ptid=1 → HOM (appears as home in game 1000 vs AWY and game 1001 vs OPP → intersect = {HOM})
    ptid=2 → AWY (appears as away in game 1000 vs HOM and game 1002 vs OPP → intersect = {AWY})
    """
    return [
        Game(
            id=1001, season=50, league="ISFL",
            home_team="HOM", away_team="OPP",
            home_team_id=1, away_team_id=3,
            plays=[
                ParsedPlay(
                    game_id=1001, quarter=1, clock="10:00",
                    play_type=PlayType.RUSH,
                    description="Rush by RB, C. for 8 yds",
                    rusher="RB, C.",
                    yards_gained=8, possession_team_id=1,
                    score_away=0, score_home=0,
                    away_team="OPP", home_team="HOM",
                ),
            ],
        ),
        Game(
            id=1002, season=50, league="ISFL",
            home_team="OPP", away_team="AWY",
            home_team_id=3, away_team_id=2,
            plays=[
                ParsedPlay(
                    game_id=1002, quarter=1, clock="10:00",
                    play_type=PlayType.RUSH,
                    description="Rush by RB, E. for 3 yds",
                    rusher="RB, E.",
                    yards_gained=3, possession_team_id=2,
                    score_away=0, score_home=0,
                    away_team="AWY", home_team="OPP",
                ),
            ],
        ),
    ]


@pytest.fixture
def client():
    """Create a test client with SQLite in-memory database."""
    from contextlib import asynccontextmanager

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_tables(engine)

    game = _make_game()
    extra_games = _make_extra_games()
    all_games = [game] + extra_games
    registry = PlayerRegistry()
    registry.build_from_games(all_games)
    load_registry(engine, registry)
    load_season(engine, all_games, registry)

    # Seed EPA data for EPA endpoint tests
    from sqlalchemy import insert, select
    with engine.begin() as conn:
        # Get play IDs
        play_rows = conn.execute(select(plays_table.c.id, plays_table.c.game_id).order_by(plays_table.c.id)).fetchall()
        for i, play_row in enumerate(play_rows):
            conn.execute(insert(play_epa_table).values(
                play_id=play_row.id, game_id=play_row.game_id, season=50,
                ep_before=1.5 - i * 0.5, ep_after=1.0 - i * 0.3, epa=-0.5 + i * 0.2,
            ))

        # Player EPA
        players_resp = conn.execute(select(plays_table.c.player_id_passer).where(plays_table.c.player_id_passer.isnot(None)).distinct()).fetchall()
        passer_id = players_resp[0][0] if players_resp else 1
        conn.execute(insert(player_season_epa_table).values(
            player_id=passer_id, player="QB, A.", team="HOM", season=50,
            pass_epa=5.0, dropbacks=150, epa_per_dropback=0.033,
            rush_epa=1.0, rush_attempts=20, epa_per_rush=0.05,
        ))

        # Team EPA
        conn.execute(insert(team_season_epa_table).values(
            team="HOM", season=50, total_epa=10.0, pass_epa=7.0, rush_epa=3.0,
            plays=200, epa_per_play=0.05,
        ))
        conn.execute(insert(team_season_epa_table).values(
            team="AWY", season=50, total_epa=-5.0, pass_epa=-3.0, rush_epa=-2.0,
            plays=180, epa_per_play=-0.028,
        ))

    # Replace lifespan to use our test engine instead of Postgres
    @asynccontextmanager
    async def test_lifespan(a):
        a.state.engine = engine
        yield

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = test_lifespan
    with TestClient(app) as c:
        yield c
    app.router.lifespan_context = original_lifespan


class TestPlaysEndpoints:
    def test_list_plays(self, client):
        resp = client.get("/plays/?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 5  # 3 from game 1000 + 1 from game 1001 + 1 from game 1002

    def test_list_plays_by_game(self, client):
        resp = client.get("/plays/1000")
        assert resp.status_code == 200
        assert len(resp.json()) == 3

    def test_filter_by_play_type(self, client):
        resp = client.get("/plays/?play_type=pass")
        assert resp.status_code == 200
        assert all(p["play_type"] == "pass" for p in resp.json())


class TestStatsEndpoints:
    def test_passing_leaders(self, client):
        resp = client.get("/stats/passing?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "QB, A."
        assert data[0]["yards"] == 45  # 15 + 30

    def test_rushing_leaders(self, client):
        resp = client.get("/stats/rushing?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "RB, C."

    def test_receiving_leaders(self, client):
        resp = client.get("/stats/receiving?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "WR, B."

    def test_team_stats(self, client):
        resp = client.get("/stats/team?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3  # HOM, AWY, OPP

    def test_player_game_log(self, client):
        # Find the player_id for QB, A.
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/stats/player/{pid}/game-log?category=passing")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1  # one game


class TestPlayersEndpoints:
    def test_search_by_name(self, client):
        resp = client.get("/players/?name=QB")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert "QB" in data[0]["canonical_name"]

    def test_get_player_profile(self, client):
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/players/{pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["player"]["canonical_name"] == "QB, A."
        assert data["career_passing"]["yards"] == 45

    def test_get_player_plays(self, client):
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/players/{pid}/plays")
        assert resp.status_code == 200
        assert len(resp.json()) >= 2  # 2 pass plays

    def test_player_not_found(self, client):
        resp = client.get("/players/999999")
        assert resp.status_code == 404


class TestEpaEndpoints:
    def test_passing_leaders(self, client):
        resp = client.get("/epa/passing-leaders?season=50&min_dropbacks=1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "QB, A."

    def test_rushing_leaders(self, client):
        resp = client.get("/epa/rushing-leaders?season=50&min_attempts=1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1

    def test_receiving_leaders_empty(self, client):
        resp = client.get("/epa/receiving-leaders?season=50&min_targets=1")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_team_epa(self, client):
        resp = client.get("/epa/team?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        # Sorted by epa_per_play desc
        assert data[0]["team"] == "HOM"

    def test_player_epa_profile(self, client):
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/epa/player/{pid}?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["pass_epa"] == 5.0

    def test_game_epa(self, client):
        resp = client.get("/epa/game/1000")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3  # 3 plays
        # At least some should have EPA
        assert any(row["epa"] is not None for row in data)

    def test_available_seasons(self, client):
        resp = client.get("/epa/seasons")
        assert resp.status_code == 200
        assert 50 in resp.json()

    def test_available_teams(self, client):
        resp = client.get("/epa/teams?season=50")
        assert resp.status_code == 200
        teams = resp.json()
        assert "HOM" in teams
        assert "AWY" in teams

    def test_available_teams_no_season(self, client):
        resp = client.get("/epa/teams")
        assert resp.status_code == 200

    def test_team_dashboard_offensive(self, client):
        resp = client.get("/epa/team-dashboard?season=50&side=offensive")
        assert resp.status_code == 200

    def test_team_dashboard_defensive(self, client):
        resp = client.get("/epa/team-dashboard?season=50&side=defensive")
        assert resp.status_code == 200

    def test_leaderboard_passing(self, client):
        resp = client.get("/epa/leaderboard?category=passing&season=50&min_plays=1")
        assert resp.status_code == 200

    def test_positions(self, client):
        resp = client.get("/epa/positions")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestEpaPlays:
    """Tests for /epa/plays endpoint: multi-player output and team+side filtering."""

    def test_plays_returns_multiple_offensive_players(self, client):
        """Pass plays should list both passer and receiver in off_players."""
        resp = client.get("/epa/plays?season=50&play_type=pass")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        play = data[0]
        # off_players should contain both passer and receiver
        roles = {p["role"] for p in play["off_players"]}
        assert "passer" in roles
        assert "receiver" in roles
        assert len(play["off_players"]) == 2

    def test_plays_rush_single_offensive_player(self, client):
        """Rush plays should list only the rusher in off_players."""
        resp = client.get("/epa/plays?season=50&play_type=rush")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        play = data[0]
        assert len(play["off_players"]) == 1
        assert play["off_players"][0]["role"] == "rusher"

    def test_plays_defensive_players(self, client):
        """Plays with a tackler should list them in def_players."""
        resp = client.get("/epa/plays?season=50&side=defensive")
        assert resp.status_code == 200
        data = resp.json()
        # The rush play has a tackler
        tackler_plays = [p for p in data if any(d["role"] == "tackler" for d in p["def_players"])]
        assert len(tackler_plays) >= 1

    def test_offensive_team_filter_shows_only_possessing_team(self, client):
        """Filtering by team on offensive side should only show plays where that team had possession."""
        # HOM has possession_team_id=1: 2 pass plays (game 1000) + 1 rush (game 1001)
        resp = client.get("/epa/plays?season=50&team=HOM&side=offensive")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3  # 2 pass + 1 rush by HOM
        assert all(p["off_team"] == "HOM" for p in data)

    def test_defensive_team_filter_shows_opponent_possessing(self, client):
        """Filtering by team on defensive side should only show plays where opponent had possession."""
        # HOM on defense = AWY had possession = 1 rush play
        resp = client.get("/epa/plays?season=50&team=HOM&side=defensive")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1  # 1 rush play where AWY had possession
        assert data[0]["def_team"] == "HOM"
        assert data[0]["off_team"] == "AWY"

    def test_offensive_team_filter_away_team(self, client):
        """AWY on offense should show rush plays where AWY had possession (ptid=2)."""
        resp = client.get("/epa/plays?season=50&team=AWY&side=offensive")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2  # 1 rush in game 1000 + 1 rush in game 1002
        assert all(p["off_team"] == "AWY" for p in data)
        assert all(p["play_type"] == "rush" for p in data)


    def test_plays_filter_by_player_offensive(self, client):
        """Filtering by player_id on offensive side returns only plays with that player."""
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/epa/plays?season=50&side=offensive&player_id={pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        # Every play should have the player as passer or receiver
        for play in data:
            off_names = [p["name"] for p in play["off_players"]]
            assert any("QB" in n for n in off_names)

    def test_plays_filter_by_player_defensive(self, client):
        """Filtering by player_id on defensive side returns only plays with that player."""
        players = client.get("/players/?name=LB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/epa/plays?season=50&side=defensive&player_id={pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        for play in data:
            def_names = [p["name"] for p in play["def_players"]]
            assert any("LB" in n for n in def_names)


class TestVizEndpoints:
    def test_viz_ep_by_distance(self, client):
        resp = client.get("/epa/viz/ep-by-distance?season_min=50&season_max=50")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        # May be empty if not enough plays per bucket, but endpoint works
        for row in data:
            assert "distance" in row
            assert "down" in row
            assert "avg_ep" in row
            assert "count" in row
            assert row["down"] in (1, 2, 3, 4)

    def test_viz_ep_by_yardline(self, client):
        resp = client.get("/epa/viz/ep-by-yardline?season_min=50&season_max=50")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for row in data:
            assert "yardline" in row
            assert "down" in row
            assert "avg_ep" in row
            assert "count" in row
            assert row["down"] in (1, 2, 3, 4)

    def test_viz_ep_by_time(self, client):
        resp = client.get("/epa/viz/ep-by-time?season_min=50&season_max=50")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for row in data:
            assert "minute" in row
            assert "down" in row
            assert "avg_ep" in row
            assert "count" in row


class TestErrorResponses:
    def test_unknown_stat_category(self, client):
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/stats/player/{pid}/game-log?category=invalid")
        assert resp.status_code == 400
