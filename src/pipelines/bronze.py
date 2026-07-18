"""
LeagueLake — BRONZE layer  (Lakeflow Declarative Pipeline)
Ingests raw Sleeper JSON from the Volume into Bronze Delta tables,
one table per endpoint, via Auto Loader. Land as-is; clean in Silver.
Runs inside a Databricks pipeline (runtime provides `spark` and `dlt`).
"""
import dlt
from pyspark.sql import functions as F

# Raw folder in the Volume — passed in from the pipeline config (the .yml).
VOLUME_ROOT = spark.conf.get("leaguelake.volume_root")

# Each Sleeper endpoint -> the subfolder holding its JSON files.
ENDPOINTS = {
    "league":          "league",
    "users":           "users",
    "rosters":         "rosters",
    "matchups":        "matchups",
    "transactions":    "transactions",
    "drafts":          "drafts",
    "draft_picks":     "draft_picks",
    "winners_bracket": "winners_bracket",
    "losers_bracket":  "losers_bracket",
}


def _read_endpoint(subfolder):
    """Auto Loader stream reading every season's files for one endpoint."""
    return (
        spark.readStream.format("cloudFiles")            # cloudFiles = Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")   # infer real types, not all strings
        .option("multiLine", "true")                     # each file is one JSON array/object
        .load(f"{VOLUME_ROOT}/season=*/{subfolder}/")    # glob across all seasons
    )


def _with_audit(df):
    """Standard Bronze audit columns."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("season", F.regexp_extract("_metadata.file_path", r"season=(\d{4})", 1))
    )


def _make_bronze(table_name, subfolder):
    """Factory: register one streaming Bronze table for an endpoint."""
    @dlt.table(
        name=f"bronze_{table_name}",
        comment=f"Raw Sleeper '{table_name}' — landed as-is via Auto Loader.",
        table_properties={"layer": "bronze"},
    )
    def _table():
        return _with_audit(_read_endpoint(subfolder))


for _name, _folder in ENDPOINTS.items():
    _make_bronze(_name, _folder)


# players_nfl.json is ONE object with ~12,200 keys -> inferring its schema would
# make a 12,200-column table. Read each file as one binary blob, decode to a JSON
# string, and parse it in Silver instead.
@dlt.table(
    name="bronze_players_raw",
    comment="Raw Sleeper player dictionary as one JSON string (exploded in Silver).",
    table_properties={"layer": "bronze"},
)
def bronze_players_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")       # whole file -> one row
        .load(f"{VOLUME_ROOT}/players_nfl/")
        .select(
            F.decode(F.col("content"), "utf-8").alias("players_json"),
            F.col("path").alias("_source_file"),
            F.current_timestamp().alias("_ingest_ts"),
        )
    )


# Raw weekly player stats (one dict keyed by player_id per season-week) — powers
# the what-if scoring simulator. Wide like the player dict, so land as one JSON
# blob per file and parse the map in Silver.
@dlt.table(
    name="bronze_player_stats",
    comment="Raw weekly NFL player stats as one JSON string per season-week (parsed in Silver).",
    table_properties={"layer": "bronze"},
)
def bronze_player_stats():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(f"{VOLUME_ROOT}/season=*/player_stats/")
        .select(
            F.decode(F.col("content"), "utf-8").alias("stats_json"),
            F.regexp_extract("path", r"season=(\d{4})", 1).alias("season"),
            F.regexp_extract("path", r"week=(\d+)", 1).cast("int").alias("week"),
            F.col("path").alias("_source_file"),
            F.current_timestamp().alias("_ingest_ts"),
        )
    )
