"""
LeagueLake — WHAT-IF SCORING SIMULATOR tables (Silver + Gold).

Only DEFENSIVE scoring is being changed, so for each team-week we need just two
things to re-score any proposed settings:
  1. the started DEF's RAW stat line  (recompute DEF points = Σ stat×setting)
  2. the team's NON-DEF points        (fixed — doesn't change with DEF settings)
Then new_team_points = non_def_points + Σ(def_stats × proposed_def_settings).

`gold_scenario_input` is exported to the portfolio app, which recomputes
standings + champions client-side as the user drags the setting sliders.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, DoubleType

# defensive scoring keys (what a DEF-settings change touches)
DEF_KEYS = ["sack", "int", "ff", "fum_rec", "fum_rec_td", "def_td", "def_st_td", "def_st_ff",
            "def_st_fum_rec", "def_2pt", "safe", "blk_kick", "pts_allow", "pts_allow_0",
            "pts_allow_1_6", "pts_allow_7_13", "pts_allow_14_20", "pts_allow_21_27",
            "pts_allow_28_34", "pts_allow_35p", "def_4_and_stop", "tkl_loss", "st_ff",
            "st_fum_rec", "st_td"]


# ---------- silver_def_stats : the started DEF's raw stats per team-week ----------
@dlt.table(name="silver_def_stats",
           comment="Per team-week: the started defense's raw stat line (for what-if re-scoring).",
           table_properties={"layer": "silver"})
def silver_def_stats():
    # parse the weekly stats blob: map<player_id, map<stat, value>>
    stats = (dlt.read("bronze_player_stats")
             .select("season", "week",
                     F.from_json("stats_json",
                                 MapType(StringType(), MapType(StringType(), DoubleType()))).alias("m")))
    # each team-week starts exactly one DEF (a team-abbreviation player_id like "SF")
    defs = (dlt.read("fact_roster_slot")
            .filter("is_starter = true AND player_id RLIKE '^[A-Z]+$'")
            .select("season", "week", "roster_id", "user_id",
                    F.col("player_id").alias("def_id"),
                    F.col("points").alias("def_actual_points")))
    return (defs.join(stats, ["season", "week"], "left")
            .withColumn("def_stats", F.element_at("m", F.col("def_id")))   # this DEF's raw stats
            .select("season", "week", "roster_id", "user_id",
                    "def_id", "def_actual_points", "def_stats"))


# ---------- gold_scenario_input : the app's per-team-week payload ----------
@dlt.table(name="gold_scenario_input",
           comment="What-if simulator input per team-week: non-DEF base points + DEF raw stats.",
           table_properties={"layer": "gold"})
def gold_scenario_input():
    fm = dlt.read("fact_matchup").select(
        "season", "week", "roster_id", "user_id", "matchup_id", "points", "is_regular")
    d = dlt.read("silver_def_stats")
    return (fm.join(d, ["season", "week", "roster_id", "user_id"], "left")
            # non-DEF points are fixed when only DEF scoring changes
            .withColumn("non_def_points",
                        F.round(F.col("points") - F.coalesce(F.col("def_actual_points"), F.lit(0.0)), 2))
            .select("season", "week", "roster_id", "user_id", "matchup_id",
                    F.col("points").alias("actual_points"), "non_def_points",
                    "def_id", "def_stats", "is_regular"))


# ---------- gold_nfl_defense_stats : every NFL defense's season-summed stats ----------
# For the "defense value board" — score any settings against these to rank all 32
# real NFL defenses. Scoring is linear, so season points = Σ(season_stat × setting).
@dlt.table(name="gold_nfl_defense_stats",
           comment="Per season, each NFL team defense's season-summed raw stats (for the value board).",
           table_properties={"layer": "gold"})
def gold_nfl_defense_stats():
    parsed = (dlt.read("bronze_player_stats")
              .select("season",
                      F.from_json("stats_json",
                                  MapType(StringType(), MapType(StringType(), DoubleType()))).alias("m")))
    # explode to one row per (season, week, player_id, stats); keep team-abbrev DEF ids
    per_week = (parsed.select("season", F.explode("m").alias("def_id", "stats"))
                .filter(F.col("def_id").rlike("^[A-Z]+$")))
    # explode inner stat map, keep DEF-relevant keys, sum across the season
    kv = (per_week.select("season", "def_id", F.explode("stats").alias("k", "v"))
          .filter(F.col("k").isin(DEF_KEYS))
          .groupBy("season", "def_id", "k").agg(F.sum("v").alias("total")))
    return (kv.groupBy("season", "def_id")
            .agg(F.map_from_entries(F.collect_list(F.struct("k", "total"))).alias("stats")))


# ---------- gold_scoring_settings : per-season scoring config (settings source of truth) ----------
# The exporter used to read scoring_settings + playoff_week_start straight from the
# raw league files, bypassing the medallion. Surface them from bronze_league so Gold
# is the single source of truth and the app can't drift from what the pipeline scored.
@dlt.table(name="gold_scoring_settings",
           comment="Per season: full scoring_settings (JSON) + playoff_week_start, from bronze_league.",
           table_properties={"layer": "gold"})
def gold_scoring_settings():
    return (dlt.read("bronze_league")
            .select("season",
                    F.to_json(F.col("scoring_settings")).alias("scoring_json"),
                    F.col("settings.playoff_week_start").cast("int").alias("playoff_week_start")))


# ---------- dq_one_def_per_team_week : structural gate for the simulator ----------
# The what-if re-scoring assumes exactly one started DEF per team-week; if a team-week
# ever had two, gold_scenario_input would fan out and the app's totals would be wrong.
# Fail the update if that invariant breaks. (reconcile.py checks it post-hoc too.)
@dlt.table(name="dq_one_def_per_team_week",
           comment="DQ: exactly one started DEF per team-week (simulator re-scoring is 1:1).",
           table_properties={"layer": "gold"})
@dlt.expect_or_fail("one_def", "cnt = 1")
def dq_one_def_per_team_week():
    return (dlt.read("silver_def_stats")
            .groupBy("season", "week", "roster_id").agg(F.count(F.lit(1)).alias("cnt")))
