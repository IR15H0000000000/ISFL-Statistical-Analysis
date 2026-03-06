import typer
from rich.console import Console

from isfl_epa.config import League

app = typer.Typer(help="ISFL play-by-play analyzer and EPA calculator")
console = Console()


@app.command()
def scrape(
    league: League = typer.Option(League.ISFL, help="League to scrape"),
    season: int = typer.Option(..., help="Season number"),
    force_refresh: bool = typer.Option(False, help="Re-download even if cached"),
):
    """Download and cache PBP and boxscore data for a season."""
    from isfl_epa.scraper.boxscore import fetch_all_season_boxscores
    from isfl_epa.scraper.pbp import fetch_all_season_pbp

    console.print(f"Scraping {league.value} Season {season}...")

    games = fetch_all_season_pbp(league, season, force_refresh=force_refresh)
    console.print(f"  PBP: {len(games)} games")

    boxscores = fetch_all_season_boxscores(league, season, force_refresh=force_refresh)
    console.print(f"  Boxscores: {len(boxscores)} games")

    console.print("[green]Done![/green]")


@app.command()
def explore(
    league: League = typer.Option(League.ISFL, help="League"),
    season: int = typer.Option(..., help="Season number"),
    game_id: int = typer.Option(..., help="Game ID to inspect"),
):
    """Dump raw play-by-play data for a single game."""
    import json

    from isfl_epa.scraper.pbp import fetch_game

    game = fetch_game(league, season, game_id)
    if game is None:
        console.print(f"[red]Game {game_id} not found[/red]")
        raise typer.Exit(1)

    console.print_json(json.dumps(game, indent=2))


@app.command()
def build(
    league: League = typer.Option(League.ISFL, help="League"),
    season: int = typer.Option(..., help="Season number"),
    database_url: str = typer.Option(None, help="PostgreSQL URL"),
):
    """Parse a season and load into PostgreSQL + Parquet."""
    from isfl_epa.config import ENGINE_CUTOFF_SEASON
    from isfl_epa.parser.play_parser import parse_game
    from isfl_epa.players.registry import PlayerRegistry
    from isfl_epa.storage.database import (
        create_tables,
        get_engine,
        load_registry,
        load_season,
    )
    from isfl_epa.storage.parquet import write_season_plays

    # Fetch PBP
    console.print(f"Building {league.value} Season {season}...")
    if season < ENGINE_CUTOFF_SEASON:
        from isfl_epa.scraper.pbp_html import fetch_all_season_pbp_html
        raw_games = fetch_all_season_pbp_html(league, season)
    else:
        from isfl_epa.scraper.pbp import fetch_all_season_pbp
        raw_games = fetch_all_season_pbp(league, season)
    console.print(f"  Fetched {len(raw_games)} games")

    # Parse
    games = [parse_game(g, season, league.value) for g in raw_games]
    console.print(f"  Parsed {len(games)} games")

    # Register players
    registry = PlayerRegistry()
    registry.build_from_games(games)
    console.print(f"  Registered {registry.player_count} players")

    # Load into PostgreSQL
    engine = get_engine(database_url)
    create_tables(engine)
    load_registry(engine, registry)
    load_season(engine, games, registry)
    console.print("  Loaded into PostgreSQL")

    # Write Parquet
    path = write_season_plays(games, season, league.value, registry)
    console.print(f"  Wrote Parquet: {path}")

    console.print("[green]Done![/green]")


@app.command()
def stats(
    league: League = typer.Option(League.ISFL, help="League"),
    season: int = typer.Option(..., help="Season number"),
    stat: str = typer.Option("passing", help="Stat category: passing, rushing, receiving, defensive, team"),
    top: int = typer.Option(20, help="Number of rows to display"),
    output: str = typer.Option(None, help="Export to file (csv or parquet)"),
):
    """Compute and display stats for a season."""
    from rich.table import Table as RichTable

    from isfl_epa.config import ENGINE_CUTOFF_SEASON
    from isfl_epa.parser.play_parser import parse_game
    from isfl_epa.stats.aggregation import season_player_stats, season_team_stats

    # Fetch and parse
    if season < ENGINE_CUTOFF_SEASON:
        from isfl_epa.scraper.pbp_html import fetch_all_season_pbp_html
        raw_games = fetch_all_season_pbp_html(league, season)
    else:
        from isfl_epa.scraper.pbp import fetch_all_season_pbp
        raw_games = fetch_all_season_pbp(league, season)

    games = [parse_game(g, season, league.value) for g in raw_games]

    if stat == "team":
        df = season_team_stats(games)
    else:
        df = season_player_stats(games, stat)

    # Export if requested
    if output:
        if output.endswith(".parquet"):
            df.to_parquet(output, index=False)
        else:
            df.to_csv(output, index=False)
        console.print(f"Exported to {output}")
        return

    # Display as rich table
    df_display = df.head(top)
    table = RichTable(title=f"{league.value} S{season} {stat.title()}")
    for col in df_display.columns:
        if col in ("game_id", "player_id"):
            continue
        table.add_column(col, justify="right" if df_display[col].dtype in ("int64", "float64") else "left")

    for _, row in df_display.iterrows():
        table.add_row(*[
            str(row[col]) for col in df_display.columns
            if col not in ("game_id", "player_id")
        ])

    console.print(table)


@app.command()
def player(
    name: str = typer.Option(None, help="Player name to search"),
    player_id: int = typer.Option(None, "--id", help="Player ID to look up"),
    database_url: str = typer.Option(None, help="PostgreSQL URL"),
):
    """Look up a player's information."""
    from rich.table import Table as RichTable
    from sqlalchemy import func, select

    from isfl_epa.storage.database import (
        get_engine,
        player_game_passing_table,
        player_names_table,
        players_table,
    )

    engine = get_engine(database_url)

    with engine.connect() as conn:
        if player_id:
            result = conn.execute(
                select(players_table).where(players_table.c.player_id == player_id)
            ).first()
            if not result:
                console.print(f"[red]Player {player_id} not found[/red]")
                raise typer.Exit(1)
            console.print(f"Player: {result.canonical_name} (ID: {result.player_id})")
            console.print(f"Seasons: {result.first_seen_season} - {result.last_seen_season}")

            # Show aliases
            aliases = conn.execute(
                select(player_names_table).where(player_names_table.c.player_id == player_id)
            ).fetchall()
            if aliases:
                table = RichTable(title="Aliases")
                table.add_column("Name")
                table.add_column("Season", justify="right")
                table.add_column("Team")
                for a in aliases:
                    table.add_row(a.name, str(a.season), a.team or "")
                console.print(table)

        elif name:
            results = conn.execute(
                select(players_table)
                .join(player_names_table, players_table.c.player_id == player_names_table.c.player_id)
                .where(func.lower(player_names_table.c.name).contains(name.lower()))
                .distinct()
                .limit(20)
            ).fetchall()

            if not results:
                console.print(f"[red]No players found matching '{name}'[/red]")
                raise typer.Exit(1)

            table = RichTable(title=f"Players matching '{name}'")
            table.add_column("ID", justify="right")
            table.add_column("Name")
            table.add_column("First Season", justify="right")
            table.add_column("Last Season", justify="right")
            for r in results:
                table.add_row(str(r.player_id), r.canonical_name,
                              str(r.first_seen_season), str(r.last_seen_season))
            console.print(table)
        else:
            console.print("[red]Provide --name or --id[/red]")
            raise typer.Exit(1)


if __name__ == "__main__":
    app()
