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
from pyspark.sql.types import MapType, StringType, LongType, DoubleType, StructType, StructField

WEEK_RE = r"week=(\d+)"   # pull the week number out of a file path


# ---------- dim_season : one row per league season ----------
@dlt.table(name="dim_season", comment="One league season: settings + playoff structure.",
           table_properties={"layer": "silver"})
@dlt.expect_or_fail("season_present", "season IS NOT NULL")   # FAIL mode: a null season is a hard error
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
@dlt.expect("points_non_negative", "points >= 0")                              # WARN modes:
@dlt.expect("valid_week", "week BETWEEN 1 AND 18")                             #   track violations,
@dlt.expect("regular_has_opponent", "NOT is_regular OR opponent_points IS NOT NULL")  # keep the rows
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


# ==================== PASS B ====================

# ---------- dim_player : explode the 12,200-player JSON dict ----------
# bronze_players_raw is ONE row with the whole player dictionary as a JSON string.
# Parse it as a map<player_id, {fields}> and explode to one row per player.
PLAYER_SCHEMA = StructType([
    StructField("full_name", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("position", StringType()),
    StructField("team", StringType()),
])

@dlt.table(name="dim_player", comment="One row per NFL player (name, position, team).",
           table_properties={"layer": "silver"})
def dim_player():
    m = F.from_json("players_json", MapType(StringType(), PLAYER_SCHEMA))
    return (dlt.read("bronze_players_raw")
            .select(F.explode(m).alias("player_id", "p"))
            .select("player_id",
                    F.coalesce(F.col("p.full_name"),
                               F.concat_ws(" ", F.col("p.first_name"), F.col("p.last_name"))
                               ).alias("full_name"),
                    F.col("p.position").alias("position"),
                    F.col("p.team").alias("team")))


# ---------- fact_roster_slot : one rostered player per team-week ----------
# Powers "points left on the bench". players_points is a wide struct, so we
# convert it struct->JSON->map to look up each player's points dynamically.
@dlt.table(name="fact_roster_slot",
           comment="One rostered player per team-week: weekly points + is_starter.",
           table_properties={"layer": "silver"})
@dlt.expect("valid_week", "week BETWEEN 1 AND 18")
def fact_roster_slot():
    ex = (dlt.read("bronze_matchups")
          .withColumn("week", F.regexp_extract("_source_file", WEEK_RE, 1).cast("int"))
          .select("season", "week", "roster_id",
                  F.col("starters"),
                  F.from_json(F.to_json("players_points"),
                              MapType(StringType(), DoubleType())).alias("pp"),
                  F.explode("players").alias("player_id")))
    r = dlt.read("bronze_rosters").select("season", "roster_id",
                                          F.col("owner_id").alias("user_id"))
    return (ex.select("season", "week", "roster_id", "player_id",
                      F.element_at("pp", F.col("player_id")).alias("points"),
                      F.array_contains("starters", F.col("player_id")).alias("is_starter"))
              .join(r, ["season", "roster_id"], "left")
              .select("season", "week", "roster_id", "user_id",
                      "player_id", "points", "is_starter"))


# ---------- fact_transaction : one add/drop item ----------
# adds/drops are wide structs keyed by player_id -> roster_id; convert to a map
# and explode. Unions the add rows and drop rows.
@dlt.table(name="fact_transaction", comment="One add/drop item per transaction.",
           table_properties={"layer": "silver"})
def fact_transaction():
    tx = dlt.read("bronze_transactions")
    add_m = F.from_json(F.to_json("adds"), MapType(StringType(), LongType()))
    drop_m = F.from_json(F.to_json("drops"), MapType(StringType(), LongType()))
    base = tx.select("transaction_id", "type", "status", "season",
                     F.col("leg").alias("week"),
                     F.col("settings.waiver_bid").alias("faab_bid"),
                     add_m.alias("add_m"), drop_m.alias("drop_m"))
    adds = base.select("transaction_id", "type", "status", "season", "week", "faab_bid",
                       F.explode("add_m").alias("player_id", "roster_id"),
                       F.lit("add").alias("action"))
    drops = base.select("transaction_id", "type", "status", "season", "week", "faab_bid",
                        F.explode("drop_m").alias("player_id", "roster_id"),
                        F.lit("drop").alias("action"))
    r = dlt.read("bronze_rosters").select("season", "roster_id",
                                          F.col("owner_id").alias("user_id"))
    return (adds.unionByName(drops)
            .join(r, ["season", "roster_id"], "left")
            .select("transaction_id", "season", "week", "type", "action",
                    "player_id", "roster_id", "user_id", "faab_bid"))


# ---------- fact_draft_pick : one pick, with auction $ ----------
@dlt.table(name="fact_draft_pick", comment="One draft pick with auction dollar amount.",
           table_properties={"layer": "silver"})
@dlt.expect("valid_auction_amount", "auction_amount BETWEEN 0 AND 200")   # WARN
@dlt.expect_or_drop("has_player", "player_id IS NOT NULL")                # DROP: a pick with no player is unusable
def fact_draft_pick():
    return dlt.read("bronze_draft_picks").select(
        "season", "draft_id", "pick_no", "round", "draft_slot",
        F.col("player_id"), F.col("roster_id"),
        F.col("picked_by").alias("user_id"),
        F.col("metadata.amount").cast("int").alias("auction_amount"))


# ---------- fact_roster_slot_quarantine : bad-record capture ----------
# Quarantine pattern: rows that pass ingestion but violate a business rule are
# routed to a separate table for inspection instead of silently kept or dropped.
# Rule: a STARTED player with no recorded points (started an inactive player).
@dlt.table(name="fact_roster_slot_quarantine",
           comment="Quarantined roster slots: a started player with NULL points (data-quality review).",
           table_properties={"layer": "silver"})
def fact_roster_slot_quarantine():
    return dlt.read("fact_roster_slot").filter("is_starter = true AND points IS NULL")
