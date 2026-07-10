"""
LeagueLake — SILVER layer, Pass A  (Lakeflow Declarative Pipeline)
Reads Bronze and builds the core star-schema pieces:
  dim_season, dim_week, dim_manager (SCD Type 2), fact_matchup.

Silver tables are materialized views (they join across rows), EXCEPT
dim_manager, which uses DLT-native CDC (apply_changes, SCD type 2).
Runs in the same pipeline as bronze.py (runtime provides `spark` + `dlt`).
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

WEEK_RE = r"week=(\d+)"   # pull the week number out of a file path


# ---------- dim_season : one row per league season ----------
@dlt.table(name="dim_season", comment="One league season: settings + playoff structure.",
           table_properties={"layer": "silver"})
def dim_season():
    return dlt.read("bronze_league").select(
        F.col("season"),
        F.col("league_id"),
        F.col("name").alias("league_name"),
        F.col("status"),
        F.col("settings.playoff_week_start").alias("playoff_week_start"),
        F.col("settings.playoff_teams").alias("playoff_teams"),
        F.col("settings.num_teams").alias("num_teams"),
    )


# ---------- dim_week : one row per (season, week) ----------
@dlt.table(name="dim_week", comment="One (season, week); regular vs playoff.",
           table_properties={"layer": "silver"})
def dim_week():
    weeks = (dlt.read("bronze_matchups")
             .select("season", F.regexp_extract("_source_file", WEEK_RE, 1).cast("int").alias("week"))
             .distinct())
    pw = dlt.read("bronze_league").select(
        "season", F.col("settings.playoff_week_start").alias("playoff_week_start"))
    return (weeks.join(pw, "season", "left")
            .withColumn("is_regular", F.col("week") < F.col("playoff_week_start"))
            .withColumn("is_playoff", F.col("week") >= F.col("playoff_week_start"))
            .select("season", "week", "is_regular", "is_playoff"))


# ---------- dim_manager : SCD Type 2 on display_name (key = user_id) ----------
# Managers can rename themselves across seasons; SCD2 preserves the history.
@dlt.view
def manager_changes():
    return dlt.read_stream("bronze_users").select(
        F.col("user_id"), F.col("display_name"), F.col("season"))


dlt.create_streaming_table(
    "dim_manager",
    comment="One row per manager per display-name period (SCD2 on display_name, key=user_id).",
    table_properties={"layer": "silver"},
)
dlt.apply_changes(
    target="dim_manager",
    source="manager_changes",
    keys=["user_id"],
    sequence_by=F.col("season"),   # order versions by season
    stored_as_scd_type=2,
    except_column_list=["season"],
)


# ---------- fact_matchup : team-week grain; median-scoring model ----------
@dlt.table(name="fact_matchup",
           comment="One row per team-week: points, opponent points, weekly median, H2H + median results.",
           table_properties={"layer": "silver"})
@dlt.expect("points_non_negative", "points >= 0")
@dlt.expect("valid_week", "week BETWEEN 1 AND 18")
def fact_matchup():
    grp = Window.partitionBy("season", "week", "matchup_id")

    mk = (dlt.read("bronze_matchups")
          .withColumn("week", F.regexp_extract("_source_file", WEEK_RE, 1).cast("int"))
          .select("season", "week", "roster_id", "matchup_id",
                  F.col("points").cast("double").alias("points")))

    # roster_id -> user_id (roster_id is season-specific, so join includes season)
    r = dlt.read("bronze_rosters").select("season", "roster_id",
                                          F.col("owner_id").alias("user_id"))
    m = mk.join(r, ["season", "roster_id"], "left")

    # opponent points = the other team's points in the 2-team matchup group
    m = (m.withColumn("group_size", F.count(F.lit(1)).over(grp))
           .withColumn("opponent_points",
                       F.when(F.col("group_size") == 2,
                              F.sum("points").over(grp) - F.col("points"))))

    # exact weekly median across all teams (avg of the two middle for 12 teams — matches Sleeper)
    med = m.groupBy("season", "week").agg(
        F.expr("percentile(points, 0.5)").alias("median_points"))
    m = m.join(med, ["season", "week"])

    # regular vs playoff boundary
    pw = dlt.read("bronze_league").select(
        "season", F.col("settings.playoff_week_start").alias("playoff_week_start"))
    m = m.join(pw, "season", "left")

    return m.select(
        "season", "week", "roster_id", "user_id", "matchup_id", "points",
        "opponent_points", "median_points",
        F.when(F.col("opponent_points").isNull(), F.lit(None))
         .when(F.col("points") > F.col("opponent_points"), "W")
         .when(F.col("points") < F.col("opponent_points"), "L")
         .otherwise("T").alias("h2h_result"),
        F.when(F.col("points") > F.col("median_points"), "W")
         .when(F.col("points") < F.col("median_points"), "L")
         .otherwise("T").alias("median_result"),
        (F.col("week") < F.col("playoff_week_start")).alias("is_regular"),
    )
