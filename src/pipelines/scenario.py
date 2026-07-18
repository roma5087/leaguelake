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
