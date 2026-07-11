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
    fm = (fm
          .withColumn("n", F.count(F.lit(1)).over(wk))
          .withColumn("rnk", F.rank().over(wk.orderBy(F.desc("points"))))
          .withColumn("ap_wins", F.col("n") - F.col("rnk"))      # teams you outscored that week
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
        .withColumn("expected_wins", F.round(F.col("all_play_pct") * (2 * F.col("reg_weeks")), 1))
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
    rs = dlt.read("fact_roster_slot")
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
    pts = (dlt.read("fact_roster_slot").groupBy("season", "player_id")
           .agg(F.round(F.sum("points"), 2).alias("season_points")))
    dpl = dlt.read("dim_player").select("player_id", "full_name", "position")
    return (dp.join(pts, ["season", "player_id"], "left")
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
