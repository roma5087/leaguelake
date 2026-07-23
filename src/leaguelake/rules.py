"""
LeagueLake — pure business-rule functions (NO Spark dependency).

These encode the risky logic in the pipeline: median scoring, all-play /
expected wins, luck, auction ROI, and the empty-slot rule. They are the
executable specification of those rules and are unit-tested in
tests/test_rules.py. The Lakeflow pipeline (silver.py / gold.py) re-implements
the SAME rules in Spark.

Because a local Spark session isn't available here, end-to-end correctness of
the Spark implementation is checked by leaguelake/reconcile.py — a set of
assertions run against the deployed lakehouse (e.g. every regular team-week has
started slots, every regular matchup group has exactly 2 teams, one DEF per
team-week, and luck is zero-sum per season). Run it with
`python -m leaguelake.reconcile` after a pipeline run.
"""
from __future__ import annotations
import re
from statistics import median as _median

EMPTY_SLOT = "0"


def is_empty_slot(player_id) -> bool:
    """Sleeper uses player_id '0' for an unfilled starter slot (gotcha #3)."""
    return player_id in (None, "0", 0)


def week_from_path(path: str | None) -> int | None:
    """Extract the integer week from a path like '.../week=8.json'."""
    m = re.search(r"week=(\d+)", path or "")
    return int(m.group(1)) if m else None


def game_result(points: float, opponent_points: float | None) -> str | None:
    """Head-to-head result vs a single opponent. None opponent (bye) -> None."""
    if opponent_points is None:
        return None
    if points > opponent_points:
        return "W"
    if points < opponent_points:
        return "L"
    return "T"


def median_points(points_list: list[float]) -> float | None:
    """Exact median (avg of the two middle for even counts) — matches Sleeper's
    median-scoring rule, which is why records reconcile."""
    if not points_list:
        return None
    return float(_median(points_list))


def median_result(points: float, median: float | None) -> str | None:
    """Result vs the weekly median."""
    if median is None:
        return None
    if points > median:
        return "W"
    if points < median:
        return "L"
    return "T"


def all_play(points: float, all_week_points: list[float]) -> tuple[float, int]:
    """All-play record for one team-week vs every other team that week.

    Returns (wins, games): wins = teams strictly outscored + 0.5 per tie;
    games = number of other teams. `all_week_points` includes this team.
    """
    others = list(all_week_points)
    others.remove(points)  # drop one occurrence of self
    wins = sum(1 for p in others if p < points) + 0.5 * sum(1 for p in others if p == points)
    return wins, len(others)


def expected_wins(all_play_wins: float, all_play_games: int, total_games: int) -> float:
    """Expected wins over `total_games` given the all-play win rate."""
    if all_play_games == 0:
        return 0.0
    return round(all_play_wins / all_play_games * total_games, 1)


def luck(actual_wins: float, expected_wins_value: float) -> float:
    """Positive = won more than skill implies (lucky); negative = unlucky."""
    return round(actual_wins - expected_wins_value, 1)


def points_per_dollar(season_points: float | None, auction_amount: int) -> float | None:
    """Auction ROI. Non-positive amount (e.g. $0 keeper) -> None."""
    if not auction_amount or auction_amount <= 0:
        return None
    return round((season_points or 0) / auction_amount, 2)
