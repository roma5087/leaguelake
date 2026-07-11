"""Unit tests for the LeagueLake business rules (pytest)."""
import pytest
from leaguelake import rules


# ---- empty starter slot (gotcha #3) ----
@pytest.mark.parametrize("pid,expected", [
    ("0", True), (0, True), (None, True),
    ("1234", False), ("SF", False),
])
def test_is_empty_slot(pid, expected):
    assert rules.is_empty_slot(pid) is expected


# ---- week extracted from file path ----
@pytest.mark.parametrize("path,week", [
    ("/Volumes/.../season=2025/matchups/week=8.json", 8),
    ("season=2023/transactions/week=14.json", 14),
    ("season=2026/league/league.json", None),
    ("", None), (None, None),
])
def test_week_from_path(path, week):
    assert rules.week_from_path(path) == week


# ---- head-to-head result ----
@pytest.mark.parametrize("pts,opp,res", [
    (110.0, 90.0, "W"),
    (90.0, 110.0, "L"),
    (100.0, 100.0, "T"),
    (100.0, None, None),   # bye — no opponent
])
def test_game_result(pts, opp, res):
    assert rules.game_result(pts, opp) == res


# ---- exact median (even vs odd) ----
def test_median_even_count_is_average_of_two_middle():
    # 12 teams -> median is the avg of the 6th and 7th values (Sleeper's rule)
    pts = [100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210]
    assert rules.median_points(pts) == 155.0  # (150+160)/2

def test_median_odd_and_edge_cases():
    assert rules.median_points([100, 200, 300]) == 200.0
    assert rules.median_points([120.5]) == 120.5
    assert rules.median_points([]) is None

@pytest.mark.parametrize("pts,med,res", [
    (160.0, 155.0, "W"), (150.0, 155.0, "L"), (155.0, 155.0, "T"),
    (100.0, None, None),
])
def test_median_result(pts, med, res):
    assert rules.median_result(pts, med) == res


# ---- all-play record ----
def test_all_play_top_scorer_beats_everyone():
    week = [90, 100, 110, 200]           # 200 is highest
    wins, games = rules.all_play(200, week)
    assert (wins, games) == (3.0, 3)     # beat all 3 others

def test_all_play_bottom_scorer_beats_nobody():
    week = [90, 100, 110, 200]
    assert rules.all_play(90, week) == (0.0, 3)

def test_all_play_counts_ties_as_half():
    week = [100, 100, 120, 80]           # this team 100, one other also 100
    wins, games = rules.all_play(100, week)
    assert (wins, games) == (1.5, 3)     # beat 80 (1) + tie 100 (0.5) = 1.5


# ---- expected wins & luck ----
def test_expected_wins_scales_all_play_rate_to_games():
    # 60% all-play over 28 games -> 16.8 expected
    assert rules.expected_wins(all_play_wins=66, all_play_games=110, total_games=28) == 16.8

def test_expected_wins_zero_games_is_zero():
    assert rules.expected_wins(0, 0, 28) == 0.0

def test_luck_is_actual_minus_expected():
    assert rules.luck(16, 14.4) == 1.6
    assert rules.luck(15, 17.8) == -2.8


# ---- auction ROI ----
@pytest.mark.parametrize("pts,amt,ppd", [
    (401.3, 33, 12.16),   # roma5087's Josh Allen
    (0.0, 5, 0.0),        # a bust
    (100.0, 0, None),     # $0 keeper -> guard against divide-by-zero
    (100.0, None, None),
])
def test_points_per_dollar(pts, amt, ppd):
    assert rules.points_per_dollar(pts, amt) == ppd
