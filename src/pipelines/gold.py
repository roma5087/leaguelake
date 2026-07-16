"""
LeagueLake — GOLD layer  (Lakeflow Declarative Pipeline)
Business-level aggregates built from the Silver star schema. Each Gold table
is a materialized view the portfolio page will visualize.
Runs in the same pipeline (runtime provides `spark` + `dlt`).
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _current_managers():
    """Current display_name per manager (the current SCD2 version)."""
    return (dlt.read("dim_manager").filter("`__END_AT` IS NULL")
            .select("user_id", "display_name"))


# ---------- gold_luck_adjusted_standings (the marquee) ----------
# Actual record vs EXPECTED record. Expected = all-play win% x games, which
# strips out schedule/median luck. luck = actual - expected.
@dlt.table(name="gold_luck_adjusted_standings",
           comment="Per team-season: actual wins vs expected (all-play) wins → luck.",
           table_properties={"layer": "gold"})
def gold_luck_adjusted_standings():
    fm = dlt.read("fact_matchup").filter(F.col("is_regular"))
    wk = Window.partitionBy("season", "week")
    tie = Window.partitionBy("season", "week", "points")
    fm = (fm
          .withColumn("n", F.count(F.lit(1)).over(wk))
          .withColumn("rnk", F.rank().over(wk.orderBy(F.desc("points"))))
          .withColumn("tie_peers", F.count(F.lit(1)).over(tie) - 1)   # teams tied with me this week
          # teams outscored, counting a tie as HALF (correct all-play semantics; matches rules.all_play).
          # `n - rnk` counts tied peers as full wins, so subtract 0.5 per tie.
          .withColumn("ap_wins", (F.col("n") - F.col("rnk")) - 0.5 * F.col("tie_peers"))
          .withColumn("ap_games", F.col("n") - 1)
          .withColumn("h2h_w", (F.col("h2h_result") == "W").cast("int"))
          .withColumn("med_w", (F.col("median_result") == "W").cast("int")))
    agg = (fm.groupBy("season", "user_id").agg(
        F.round(F.sum("points"), 2).alias("points_for"),
        (F.sum("h2h_w") + F.sum("med_w")).alias("actual_wins"),
        F.sum("ap_wins").alias("all_play_wins"),
        F.sum("ap_games").alias("all_play_games"),
        F.countDistinct("week").alias("reg_weeks"))
        .withColumn("all_play_pct", F.round(F.col("all_play_wins") / F.col("all_play_games"), 3))
        # use the RAW ratio (not the rounded pct) so expected_wins matches rules.expected_wins
        .withColumn("expected_wins",
                    F.round(F.col("all_play_wins") / F.col("all_play_games") * (2 * F.col("reg_weeks")), 1))
        .withColumn("luck", F.round(F.col("actual_wins") - F.col("expected_wins"), 1)))
    return (agg.join(_current_managers(), "user_id", "left")
            .select("season", "display_name", "points_for", "actual_wins",
                    "expected_wins", "luck", "all_play_pct"))


# ---------- gold_manager_consistency ----------
@dlt.table(name="gold_manager_consistency",
           comment="Per team-season: weekly scoring average and variability.",
           table_properties={"layer": "gold"})
def gold_manager_consistency():
    fm = dlt.read("fact_matchup").filter(F.col("is_regular"))
    return (fm.groupBy("season", "user_id").agg(
        F.round(F.avg("points"), 2).alias("avg_points"),
        F.round(F.stddev("points"), 2).alias("stddev_points"),
        F.round(F.min("points"), 2).alias("min_points"),
        F.round(F.max("points"), 2).alias("max_points"))
        .withColumn("coeff_variation", F.round(F.col("stddev_points") / F.col("avg_points"), 3))
        .join(_current_managers(), "user_id", "left")
        .select("season", "display_name", "avg_points", "stddev_points",
                "min_points", "max_points", "coeff_variation"))


# ---------- gold_bench_points ----------
# Position-agnostic proxy: per team-week, if your best bench player outscored
# your worst starter, that's points left on the bench.
@dlt.table(name="gold_bench_points",
           comment="Points left on the bench per team-season (position-agnostic proxy).",
           table_properties={"layer": "gold"})
def gold_bench_points():
    # regular season only — playoff/consolation weeks have dead lineups (~37% noise)
    reg_weeks = dlt.read("dim_week").filter(F.col("is_regular")).select("season", "week")
    rs = dlt.read("fact_roster_slot").join(reg_weeks, ["season", "week"])
    tw = (rs.groupBy("season", "week", "roster_id", "user_id").agg(
        F.sum(F.when(F.col("is_starter"), F.col("points")).otherwise(0.0)).alias("starter_points"),
        F.min(F.when(F.col("is_starter"), F.col("points"))).alias("worst_starter"),
        F.max(F.when(~F.col("is_starter"), F.col("points"))).alias("best_bench"))
        .withColumn("points_left",
                    F.greatest(F.lit(0.0), F.col("best_bench") - F.col("worst_starter"))))
    return (tw.groupBy("season", "user_id").agg(
        F.round(F.sum("starter_points"), 2).alias("starter_points"),
        F.round(F.sum("points_left"), 2).alias("points_left_on_bench"))
        .join(_current_managers(), "user_id", "left")
        .select("season", "display_name", "starter_points", "points_left_on_bench"))


# ---------- gold_auction_roi ----------
# Season points per draft dollar. season_points = a player's total league output
# that season (from fact_roster_slot). ROI = points / auction_amount.
@dlt.table(name="gold_auction_roi",
           comment="Auction draft value: season points per dollar spent.",
           table_properties={"layer": "gold"})
@dlt.expect("positive_amount", "auction_amount > 0")
def gold_auction_roi():
    dp = dlt.read("fact_draft_pick").filter(F.col("auction_amount") > 0)
    # points earned ON THE DRAFTING ROSTER only (group by roster_id too), so a
    # player who was traded/dropped doesn't credit another team's points to the drafter.
    pts = (dlt.read("fact_roster_slot").groupBy("season", "player_id", "roster_id")
           .agg(F.round(F.sum("points"), 2).alias("season_points")))
    dpl = dlt.read("dim_player").select("player_id", "full_name", "position")
    return (dp.join(pts, ["season", "player_id", "roster_id"], "left")
            .join(dpl, "player_id", "left")
            .withColumn("season_points", F.coalesce(F.col("season_points"), F.lit(0.0)))
            .withColumn("points_per_dollar", F.round(F.col("season_points") / F.col("auction_amount"), 2))
            .join(_current_managers(), "user_id", "left")
            .select("season", "display_name", "full_name", "position",
                    "auction_amount", "season_points", "points_per_dollar"))


# ---------- gold_h2h_matrix ----------
# Head-to-head record between every pair of managers (regular season), via a
# self-join of fact_matchup on the shared matchup_id.
@dlt.table(name="gold_h2h_matrix",
           comment="Head-to-head record between managers (regular season).",
           table_properties={"layer": "gold"})
def gold_h2h_matrix():
    fm = dlt.read("fact_matchup").filter(F.col("is_regular") & F.col("matchup_id").isNotNull())
    a = fm.select("season", "week", "matchup_id",
                  F.col("user_id").alias("mgr"), F.col("points").alias("pts"))
    b = fm.select("season", "week", "matchup_id",
                  F.col("user_id").alias("opp"), F.col("points").alias("opp_pts"))
    j = a.join(b, ["season", "week", "matchup_id"]).filter(F.col("mgr") != F.col("opp"))
    mgr = _current_managers().select(F.col("user_id").alias("mgr"),
                                     F.col("display_name").alias("manager"))
    opp = _current_managers().select(F.col("user_id").alias("opp"),
                                     F.col("display_name").alias("opponent"))
    return (j.withColumn("win", (F.col("pts") > F.col("opp_pts")).cast("int"))
            .withColumn("loss", (F.col("pts") < F.col("opp_pts")).cast("int"))
            .groupBy("mgr", "opp").agg(
                F.sum("win").alias("wins"),
                F.sum("loss").alias("losses"),
                F.count(F.lit(1)).alias("games"))
            .join(mgr, "mgr", "left").join(opp, "opp", "left")
            .select("manager", "opponent", "wins", "losses", "games"))


# ---------- gold_dynasty (all-time leaderboard) ----------
# One row per manager across ALL seasons: career record, finishes, playoff
# appearances, and championships (winner of the p=1 bracket node). Managers
# persist across seasons via user_id (roster_id is not stable — gotcha #1).
@dlt.table(name="gold_dynasty",
           comment="All-time leaderboard: career record, finishes, playoff runs, championships.",
           table_properties={"layer": "gold"})
def gold_dynasty():
    # per-season regular-season standings from fact_matchup
    fm = dlt.read("fact_matchup").filter(F.col("is_regular"))
    per = fm.groupBy("season", "user_id").agg(
        (F.sum((F.col("h2h_result") == "W").cast("int"))
         + F.sum((F.col("median_result") == "W").cast("int"))).alias("wins"),
        (F.sum((F.col("h2h_result") == "L").cast("int"))
         + F.sum((F.col("median_result") == "L").cast("int"))).alias("losses"),
        F.round(F.sum("points"), 2).alias("points_for"))
    rank_w = Window.partitionBy("season").orderBy(F.desc("wins"), F.desc("points_for"))
    per = per.withColumn("reg_rank", F.row_number().over(rank_w))
    playoff_teams = dlt.read("dim_season").select("season", "playoff_teams")
    per = (per.join(playoff_teams, "season", "left")
           .withColumn("made_playoffs", (F.col("reg_rank") <= F.col("playoff_teams")).cast("int")))

    # championships: winner (w) of the p=1 node, mapped roster_id -> user_id,
    # restricted to COMPLETE seasons (ignore the 2026 pre-draft skeleton)
    complete = dlt.read("dim_season").filter(F.col("status") == "complete").select("season")
    rosters = dlt.read("bronze_rosters").select("season", "roster_id",
                                                F.col("owner_id").alias("user_id"))
    champs = (dlt.read("bronze_winners_bracket").filter(F.col("p") == 1)
              .join(complete, "season")
              .select("season", F.col("w").alias("roster_id"))
              .join(rosters, ["season", "roster_id"], "left")
              .groupBy("user_id").agg(F.count(F.lit(1)).alias("championships")))

    agg = (per.groupBy("user_id").agg(
        F.countDistinct("season").alias("seasons"),
        F.sum("wins").alias("career_wins"),
        F.sum("losses").alias("career_losses"),
        F.round(F.sum("points_for"), 2).alias("total_points_for"),
        F.round(F.avg("reg_rank"), 1).alias("avg_finish"),
        F.min("reg_rank").alias("best_finish"),
        F.max("reg_rank").alias("worst_finish"),
        F.sum("made_playoffs").alias("playoff_appearances"))
        .withColumn("career_win_pct",
                    F.round(F.col("career_wins") / (F.col("career_wins") + F.col("career_losses")), 3)))

    return (agg.join(champs, "user_id", "left")
            .withColumn("championships", F.coalesce(F.col("championships"), F.lit(0)))
            .join(_current_managers(), "user_id", "left")
            .select("display_name", "seasons", "career_wins", "career_losses", "career_win_pct",
                    "total_points_for", "avg_finish", "best_finish", "worst_finish",
                    "playoff_appearances", "championships"))
