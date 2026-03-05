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


if __name__ == "__main__":
    app()
