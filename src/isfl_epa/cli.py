from pathlib import Path

import typer
from rich.console import Console

from isfl_epa.config import League

app = typer.Typer(help="ISFL play-by-play analyzer and EPA calculator")
console = Console()


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable DEBUG logging"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress INFO logging (WARNING+ only)"),
):
    """ISFL play-by-play analyzer and EPA calculator."""
    from isfl_epa.logging_config import setup_logging

    if verbose:
        level = "DEBUG"
    elif quiet:
        level = "WARNING"
    else:
        level = "INFO"
    setup_logging(level, rich_handler=True)


@app.command()
def scrape(
    league: League = typer.Option(League.ISFL, help="League to scrape"),
    season: int = typer.Option(..., help="Season number"),
    force_refresh: bool = typer.Option(False, help="Re-download even if cached"),
):
    """Download and cache PBP and boxscore data for a season."""
    from isfl_epa.config import ENGINE_CUTOFF_SEASON

    console.print(f"Scraping {league.value} Season {season}...")

    if season < ENGINE_CUTOFF_SEASON:
        from isfl_epa.scraper.boxscore_html import fetch_all_season_boxscores_html
        from isfl_epa.scraper.pbp_html import fetch_all_season_pbp_html

        games = fetch_all_season_pbp_html(league, season, force_refresh=force_refresh)
        console.print(f"  PBP: {len(games)} games")

        boxscores = fetch_all_season_boxscores_html(league, season, force_refresh=force_refresh)
        console.print(f"  Boxscores: {len(boxscores)} games")
    else:
        from isfl_epa.scraper.boxscore import fetch_all_season_boxscores
        from isfl_epa.scraper.pbp import fetch_all_season_pbp

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
        init_registry_from_db,
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

    # Set up DB and seed registry with existing players so IDs are stable across seasons
    engine = get_engine(database_url)
    create_tables(engine)
    registry = PlayerRegistry()
    init_registry_from_db(engine, registry)
    registry.build_from_games(games)
    console.print(f"  Registered {registry.player_count} players")

    # Load into PostgreSQL
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


@app.command()
def summary(
    season: int = typer.Option(None, help="Filter to a single season"),
    database_url: str = typer.Option(None, help="PostgreSQL URL"),
):
    """Show a summary of all parsed data in the database."""
    from rich.table import Table as RichTable
    from sqlalchemy import Integer, distinct, func, select

    from isfl_epa.storage.database import get_engine, players_table, plays_table

    engine = get_engine(database_url)
    t = plays_table

    with engine.connect() as conn:
        # Build season filter
        where = t.c.season == season if season else True

        # Overall totals
        totals = conn.execute(
            select(
                func.count(distinct(t.c.season)).label("seasons"),
                func.count(distinct(t.c.game_id)).label("games"),
                func.count().label("plays"),
            ).where(where)
        ).first()

        player_count = conn.execute(
            select(func.count()).select_from(players_table)
        ).scalar()

        console.print()
        console.print("[bold]Parsed Data Summary[/bold]")
        console.print(f"  Seasons: {totals.seasons}")
        console.print(f"  Games:   {totals.games:,}")
        console.print(f"  Plays:   {totals.plays:,}")
        console.print(f"  Players: {player_count:,}")

        # Play type breakdown
        type_rows = conn.execute(
            select(
                t.c.play_type,
                func.count().label("count"),
            )
            .where(where)
            .group_by(t.c.play_type)
            .order_by(func.count().desc())
        ).fetchall()

        type_table = RichTable(title="Play Type Breakdown")
        type_table.add_column("Play Type")
        type_table.add_column("Count", justify="right")
        type_table.add_column("Pct", justify="right")
        for row in type_rows:
            pct = row.count / totals.plays * 100 if totals.plays else 0
            type_table.add_row(row.play_type, f"{row.count:,}", f"{pct:.1f}%")
        console.print()
        console.print(type_table)

        # Per-season table
        season_rows = conn.execute(
            select(
                t.c.season,
                func.count(distinct(t.c.game_id)).label("games"),
                func.count().label("plays"),
                func.sum(func.cast(t.c.play_type == "pass", Integer)).label("pass_plays"),
                func.sum(func.cast(t.c.play_type == "rush", Integer)).label("rush_plays"),
                func.sum(func.cast(t.c.touchdown == True, Integer)).label("tds"),  # noqa: E712
                func.sum(func.cast(t.c.interception == True, Integer)).label("ints"),  # noqa: E712
            )
            .where(where)
            .group_by(t.c.season)
            .order_by(t.c.season)
        ).fetchall()

        season_table = RichTable(title="Per-Season Summary")
        season_table.add_column("Season", justify="right")
        season_table.add_column("Games", justify="right")
        season_table.add_column("Plays", justify="right")
        season_table.add_column("Pass", justify="right")
        season_table.add_column("Rush", justify="right")
        season_table.add_column("TDs", justify="right")
        season_table.add_column("INTs", justify="right")
        for row in season_rows:
            season_table.add_row(
                str(row.season),
                str(row.games),
                f"{row.plays:,}",
                f"{row.pass_plays or 0:,}",
                f"{row.rush_plays or 0:,}",
                str(row.tds or 0),
                str(row.ints or 0),
            )
        console.print()
        console.print(season_table)


@app.command()
def train_ep(
    model_type: str = typer.Option("hgb_reg", help="Model type: hgb_reg, hgb, or logistic"),
    league: League = typer.Option(League.ISFL),
    era: str = typer.Option("both", help="Era to train: 2016, 2022, or both"),
):
    """Train era-specific Expected Points models."""
    from isfl_epa.epa.dataset import (
        build_drive_feature_matrix,
        build_era_feature_matrix,
        label_drive_outcome,
        label_next_score,
        load_training_plays,
    )
    from isfl_epa.epa.model import MODEL_2016_PATH, MODEL_2022_PATH, EPModel

    is_regression = model_type == "hgb_reg"

    from isfl_epa.config import (
        TEST_SEASON_2022_STEP,
        TEST_SEASONS_2016,
        TRAIN_SEASONS_2016,
    )

    eras = {}
    if era in ("2016", "both"):
        eras["2016"] = {
            "train_seasons": TRAIN_SEASONS_2016,
            "test_seasons": TEST_SEASONS_2016,
            "save_path": MODEL_2016_PATH,
        }
    if era in ("2022", "both"):
        test_2022 = list(range(27, 60, TEST_SEASON_2022_STEP))
        train_2022 = [s for s in range(27, 60) if s not in test_2022]
        eras["2022"] = {
            "train_seasons": train_2022,
            "test_seasons": test_2022,
            "save_path": MODEL_2022_PATH,
        }

    for era_name, cfg in eras.items():
        _train_era(
            era_name, cfg, model_type, league.value, is_regression,
            load_training_plays, label_drive_outcome, label_next_score,
            build_drive_feature_matrix, build_era_feature_matrix, EPModel,
        )


def _train_era(
    era_name, cfg, model_type, league_value, is_regression,
    load_training_plays, label_drive_outcome, label_next_score,
    build_drive_feature_matrix, build_era_feature_matrix, EPModel,
):
    """Train and evaluate a single era model."""
    console.print(f"\n[bold]Training {era_name} era model ({model_type})[/bold]")

    console.print(f"  Loading training data ({len(cfg['train_seasons'])} seasons)...")
    train_df = load_training_plays(cfg["train_seasons"], league_value)
    console.print(f"  {len(train_df):,} plays loaded")

    if is_regression:
        console.print("  Labeling drive outcomes...")
        train_df = label_drive_outcome(train_df)
        console.print("  Building feature matrix...")
        X_train, y_train, _, is_start = build_drive_feature_matrix(train_df)
        X_train = X_train[is_start]
        y_train = y_train[is_start]
    else:
        console.print("  Labeling next-score events...")
        train_df = label_next_score(train_df)
        console.print("  Building feature matrix...")
        X_train, y_train = build_era_feature_matrix(train_df)
    console.print(f"  {len(X_train):,} training samples")

    console.print(f"  Loading test data ({len(cfg['test_seasons'])} seasons)...")
    test_df = load_training_plays(cfg["test_seasons"], league_value)
    if is_regression:
        test_df = label_drive_outcome(test_df)
        X_test, y_test, _, is_start_test = build_drive_feature_matrix(test_df)
        X_test = X_test[is_start_test]
        y_test = y_test[is_start_test]
    else:
        test_df = label_next_score(test_df)
        X_test, y_test = build_era_feature_matrix(test_df)
    console.print(f"  {len(X_test):,} test samples")

    ep = EPModel()
    train_metrics = ep.train(X_train, y_train, model_type=model_type)
    test_metrics = ep.evaluate(X_test, y_test)

    if is_regression:
        console.print(f"  Train MAE: {train_metrics['train_mae']:.4f}")
        console.print(f"  Test MAE:  {test_metrics['mae']:.4f}")
        console.print(f"  Test R²:   {test_metrics['r2']:.4f}")
    else:
        console.print(f"  Train log-loss: {train_metrics['train_log_loss']:.4f}")
        console.print(f"  Test log-loss:  {test_metrics['log_loss']:.4f}")

    ep.save(cfg["save_path"])
    console.print(f"  [green]Saved to {cfg['save_path']}[/green]")


@app.command()
def compute_epa(
    league: League = typer.Option(League.ISFL),
    season: int = typer.Option(..., help="Season to compute EPA for"),
    model_path: str = typer.Option(None, help="Path to trained EP model"),
    database_url: str = typer.Option(None, help="PostgreSQL URL"),
):
    """Compute EPA for all plays in a season."""
    from isfl_epa.epa.calculator import compute_epa_for_season
    from isfl_epa.epa.model import DEFAULT_MODEL_PATH, EPModel, EPModelPair
    from isfl_epa.storage.database import (
        create_tables,
        get_engine,
        load_epa_season,
    )
    from isfl_epa.storage.parquet import write_epa_results

    if model_path:
        console.print(f"Loading EP model from {model_path}...")
        ep_model = EPModel.load(model_path)
    else:
        console.print("Loading era-specific EP models...")
        ep_model = EPModelPair.load()

    console.print(f"Computing EPA for {league.value} S{season}...")
    epa_df = compute_epa_for_season(season, league.value, ep_model)
    valid_count = epa_df["epa"].notna().sum()
    console.print(f"  {valid_count:,} plays with EPA (of {len(epa_df):,} total)")

    if valid_count > 0:
        mean_epa = epa_df["epa"].dropna().mean()
        console.print(f"  Mean EPA: {mean_epa:.4f}")

    # Write Parquet
    parquet_path = write_epa_results(epa_df, season, league.value)
    console.print(f"  Wrote Parquet: {parquet_path}")

    # Load into PostgreSQL
    engine = get_engine(database_url)
    create_tables(engine)
    load_epa_season(engine, epa_df, season)
    console.print("  Loaded into PostgreSQL")

    console.print("[green]Done![/green]")


@app.command()
def epa_stats(
    league: League = typer.Option(League.ISFL),
    season: int = typer.Option(..., help="Season"),
    stat: str = typer.Option("passing", help="Category: passing, rushing, receiving, team"),
    top: int = typer.Option(20, help="Number of rows to display"),
    database_url: str = typer.Option(None, help="PostgreSQL URL"),
):
    """Display EPA leaders for a season."""
    from rich.table import Table as RichTable
    from sqlalchemy import desc, select

    from isfl_epa.storage.database import (
        get_engine,
        player_season_epa_table,
        team_season_epa_table,
    )

    engine = get_engine(database_url)

    with engine.connect() as conn:
        if stat == "team":
            rows = conn.execute(
                select(team_season_epa_table)
                .where(team_season_epa_table.c.season == season)
                .order_by(desc(team_season_epa_table.c.epa_per_play))
            ).fetchall()

            table = RichTable(title=f"{league.value} S{season} Team EPA")
            table.add_column("Team")
            table.add_column("Total EPA", justify="right")
            table.add_column("Pass EPA", justify="right")
            table.add_column("Rush EPA", justify="right")
            table.add_column("Plays", justify="right")
            table.add_column("EPA/Play", justify="right")
            for r in rows:
                table.add_row(
                    r.team,
                    f"{r.total_epa:.1f}",
                    f"{r.pass_epa:.1f}",
                    f"{r.rush_epa:.1f}",
                    str(r.plays),
                    f"{r.epa_per_play:.3f}",
                )
            console.print(table)
        else:
            t = player_season_epa_table
            from isfl_epa.config import MIN_DROPBACKS, MIN_RUSH_ATTEMPTS, MIN_TARGETS

            col_map = {
                "passing": ("pass_epa", "dropbacks", "epa_per_dropback", MIN_DROPBACKS),
                "rushing": ("rush_epa", "rush_attempts", "epa_per_rush", MIN_RUSH_ATTEMPTS),
                "receiving": ("recv_epa", "targets", "epa_per_target", MIN_TARGETS),
            }
            if stat not in col_map:
                console.print(f"[red]Unknown stat: {stat}[/red]")
                raise typer.Exit(1)

            epa_col, count_col, rate_col, min_plays = col_map[stat]
            rows = conn.execute(
                select(t)
                .where(t.c.season == season)
                .where(getattr(t.c, count_col) >= min_plays)
                .order_by(desc(getattr(t.c, rate_col)))
                .limit(top)
            ).fetchall()

            table = RichTable(title=f"{league.value} S{season} {stat.title()} EPA")
            table.add_column("Player")
            table.add_column("Team")
            table.add_column("Total EPA", justify="right")
            table.add_column("Plays", justify="right")
            table.add_column("EPA/Play", justify="right")
            for r in rows:
                table.add_row(
                    r.player,
                    r.team or "",
                    f"{getattr(r, epa_col):.1f}",
                    str(getattr(r, count_col)),
                    f"{getattr(r, rate_col):.3f}",
                )
            console.print(table)


@app.command()
def scrape_rosters(
    league: League = typer.Option(League.ISFL, help="League"),
    season: int = typer.Option(None, help="Single season to scrape"),
    season_range: str = typer.Option(None, help="Season range, e.g. '1-59'"),
    force_refresh: bool = typer.Option(False, help="Re-download even if cached"),
    database_url: str = typer.Option(None, help="PostgreSQL URL"),
):
    """Scrape team roster pages and load player positions into the database."""
    from sqlalchemy import select

    from isfl_epa.scraper.roster import (
        TEAM_ABBR_MAP,
        fetch_season_rosters,
        match_roster_to_players,
    )
    from isfl_epa.storage.database import (
        create_tables,
        get_engine,
        get_team_id_to_abbr,
        load_player_positions,
        player_names_table,
    )

    if season is None and season_range is None:
        console.print("[red]Provide --season or --season-range[/red]")
        raise typer.Exit(1)

    if season_range:
        parts = season_range.split("-")
        seasons = list(range(int(parts[0]), int(parts[1]) + 1))
    else:
        seasons = [season]

    engine = get_engine(database_url)
    create_tables(engine)

    total_matched = 0
    total_unmatched = 0

    for s in seasons:
        result = _scrape_and_load_season_roster(
            engine, league, s, force_refresh,
            select, player_names_table, TEAM_ABBR_MAP,
            fetch_season_rosters, match_roster_to_players,
            get_team_id_to_abbr, load_player_positions,
        )
        if result:
            total_matched += result["matched"]
            total_unmatched += result["unmatched"]

    console.print(f"\n[bold]Total: {total_matched} matched, {total_unmatched} unmatched[/bold]")
    console.print("[green]Done![/green]")


def _scrape_and_load_season_roster(
    engine, league, season, force_refresh,
    select, player_names_table, TEAM_ABBR_MAP,
    fetch_season_rosters, match_roster_to_players,
    get_team_id_to_abbr, load_player_positions,
) -> dict | None:
    """Scrape and load roster data for a single season. Returns match counts or None."""
    console.print(f"Scraping {league.value} S{season} rosters...")
    roster, team_names = fetch_season_rosters(league, season, force_refresh=force_refresh)
    if not roster:
        console.print("  [yellow]No roster data found[/yellow]")
        return None

    console.print(f"  Found {len(roster)} players across all teams")

    # Build team_id -> abbreviation mapping (primary: DB, fallback: page name)
    team_id_map = get_team_id_to_abbr(engine, season)
    for tid, tname in team_names.items():
        if tid not in team_id_map:
            abbr = TEAM_ABBR_MAP.get(tname)
            if abbr:
                team_id_map[tid] = abbr

    if team_id_map:
        console.print(f"  Teams: {', '.join(f'{v} (id={k})' for k, v in sorted(team_id_map.items()))}")

    # Load player_names for this season to match against
    with engine.connect() as conn:
        pn_rows = conn.execute(
            select(player_names_table).where(player_names_table.c.season == season)
        ).fetchall()

    player_names = [
        {"player_id": r.player_id, "name": r.name, "team": r.team}
        for r in pn_rows
    ]

    roster = match_roster_to_players(roster, player_names)

    # Assign team from plays data, not roster page (trade handling)
    pid_team = {r.player_id: r.team for r in pn_rows if r.team}
    for entry in roster:
        pid = entry.get("player_id")
        if pid and pid in pid_team:
            entry["team"] = pid_team[pid]
        elif not entry.get("team"):
            tid = entry.get("team_id")
            if tid and tid in team_id_map:
                entry["team"] = team_id_map[tid]

    result = load_player_positions(engine, roster, season)
    console.print(f"  Matched: {result['matched']}, Unmatched: {result['unmatched']}")
    return result


@app.command("detect-duplicates")
def detect_duplicates():
    """Detect players with duplicate IDs (same normalized name, different player_ids)."""
    from rich.table import Table

    from isfl_epa.storage.database import find_duplicate_players, get_engine

    engine = get_engine()
    duplicates = find_duplicate_players(engine)

    if not duplicates:
        console.print("[green]No duplicate players found![/green]")
        return

    table = Table(title=f"Duplicate Players ({len(duplicates)} groups)")
    table.add_column("Normalized Name", style="cyan")
    table.add_column("Keep ID", style="green")
    table.add_column("Remove IDs", style="red")
    table.add_column("Raw Names")

    for d in duplicates:
        table.add_row(
            d["normalized_name"],
            str(d["keep_id"]),
            ", ".join(str(x) for x in d["remove_ids"]),
            " | ".join(d["names"]),
        )

    console.print(table)
    total_removals = sum(len(d["remove_ids"]) for d in duplicates)
    console.print(f"\n[bold]{total_removals} player IDs to merge across {len(duplicates)} groups[/bold]")


@app.command("merge-duplicates")
def merge_duplicates(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview merges without executing"),
):
    """Merge duplicate player IDs into canonical entries."""
    from isfl_epa.storage.database import find_duplicate_players, get_engine, merge_players_db

    engine = get_engine()
    duplicates = find_duplicate_players(engine)

    if not duplicates:
        console.print("[green]No duplicate players found![/green]")
        return

    # Build list of (keep_id, remove_id) pairs
    merge_pairs = []
    for d in duplicates:
        keep_id = d["keep_id"]
        for remove_id in d["remove_ids"]:
            merge_pairs.append((keep_id, remove_id))

    if dry_run:
        for keep_id, remove_id in merge_pairs[:20]:
            console.print(f"  [dim]Would merge {remove_id} → {keep_id}[/dim]")
        if len(merge_pairs) > 20:
            console.print(f"  [dim]... and {len(merge_pairs) - 20} more[/dim]")
        console.print(f"\n[bold yellow]Dry run: {len(merge_pairs)} merges would be performed[/bold yellow]")
    else:
        console.print(f"Merging {len(merge_pairs)} duplicate player IDs...")
        count = merge_players_db(engine, merge_pairs)
        console.print(f"\n[bold green]{count} merges completed[/bold green]")
        console.print("[dim]Re-run build + compute-epa for affected seasons to regenerate stats[/dim]")


if __name__ == "__main__":
    app()
