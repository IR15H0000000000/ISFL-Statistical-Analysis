"""Reconstruct running scores for S1-26 games from scoring events.

S1-26 HTML PBP pages lack per-play score data (score_away/score_home are
None). This module walks through plays chronologically and tracks the
running score from touchdowns, PATs, field goals, and safeties.
"""

from __future__ import annotations

from isfl_epa.parser.schema import Game, PlayType


def reconstruct_game_scores(game: Game) -> Game:
    """Backfill score_away/score_home on each play from scoring events.

    Only modifies plays that have None scores. Games that already have
    score data (S27-59) are returned unchanged.
    """
    # Skip if first non-marker play already has score data
    for p in game.plays:
        if p.play_type not in (PlayType.QUARTER_MARKER, PlayType.TIMEOUT):
            if p.score_away is not None:
                return game
            break

    if not game.home_team_id or not game.away_team_id:
        return game

    home_score = 0
    away_score = 0

    for play in game.plays:
        # Set pre-play score
        play.score_home = home_score
        play.score_away = away_score

        # Determine which side scored
        is_home_possession = play.possession_team_id == game.home_team_id
        is_away_possession = play.possession_team_id == game.away_team_id

        if play.touchdown:
            if is_home_possession:
                home_score += 6
            elif is_away_possession:
                away_score += 6

        if play.pat_good is True:
            if is_home_possession:
                home_score += 1
            elif is_away_possession:
                away_score += 1

        if play.fg_good is True:
            if is_home_possession:
                home_score += 3
            elif is_away_possession:
                away_score += 3

        if play.safety:
            # Safety awards 2 points to the defensive team (opponent)
            if is_home_possession:
                away_score += 2
            elif is_away_possession:
                home_score += 2

    # Also populate away_team / home_team names on each play
    for play in game.plays:
        if play.away_team is None:
            play.away_team = game.away_team
        if play.home_team is None:
            play.home_team = game.home_team

    return game


def get_final_score(game: Game) -> tuple[int, int]:
    """Return (away_score, home_score) from the last play."""
    for play in reversed(game.plays):
        if play.score_away is not None and play.score_home is not None:
            # Return post-game score by re-adding any scoring on the last play
            away = play.score_away
            home = play.score_home

            is_home = play.possession_team_id == game.home_team_id
            is_away = play.possession_team_id == game.away_team_id

            if play.touchdown:
                if is_home:
                    home += 6
                elif is_away:
                    away += 6
            if play.pat_good is True:
                if is_home:
                    home += 1
                elif is_away:
                    away += 1
            if play.fg_good is True:
                if is_home:
                    home += 3
                elif is_away:
                    away += 3
            if play.safety:
                if is_home:
                    away += 2
                elif is_away:
                    home += 2

            return away, home
    return 0, 0
