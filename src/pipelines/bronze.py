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

# Append-only endpoints — each pull writes new per-week files (matchups/transactions)
# or one immutable file per season (drafts/brackets), so plain Auto Loader append is
# correct. The MUTABLE endpoints (league, rosters, players) are handled separately
# below with allowOverwrites + SCD1 so a re-pull updates in place (see note there).
ENDPOINTS = {
    "users":           "users",
    "matchups":        "matchups",
    "transactions":    "transactions",
    "drafts":          "drafts",
    "draft_picks":     "draft_picks",
    "winners_bracket": "winners_bracket",
    "losers_bracket":  "losers_bracket",
}


def _read_endpoint(subfolder, allow_overwrites=False):
    """Auto Loader stream reading every season's files for one endpoint."""
    reader = (
        spark.readStream.format("cloudFiles")            # cloudFiles = Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")   # infer real types, not all strings
        .option("multiLine", "true")                     # each file is one JSON array/object
    )
    if allow_overwrites:
        # re-ingest a file whose contents changed since last pull (mutable endpoints)
        reader = reader.option("cloudFiles.allowOverwrites", "true")
    return reader.load(f"{VOLUME_ROOT}/season=*/{subfolder}/")   # glob across all seasons


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


# ==================== MUTABLE ENDPOINTS — idempotent on re-pull ====================
# The collector overwrites league/rosters/players each pull (stable paths). Plain
# Auto Loader append would either go STALE (default: an overwritten file is never
# re-read) or DOUBLE-COUNT (allowOverwrites=true with no dedup -> two rows per key
# -> downstream joins fan out). Fix: read with allowOverwrites=true so a changed
# file IS re-ingested, then apply_changes (SCD type 1) collapses to the latest row
# per natural key. Table names are unchanged, so Silver/Gold read them as before.

# ---- bronze_league : one row per season (key = season) ----
@dlt.view
def _src_league():
    return _with_audit(_read_endpoint("league", allow_overwrites=True))

dlt.create_streaming_table(
    "bronze_league",
    comment="Raw Sleeper 'league' — SCD1 latest per season (idempotent on re-pull).",
    table_properties={"layer": "bronze"},
)
dlt.apply_changes(target="bronze_league", source="_src_league",
                  keys=["season"], sequence_by=F.col("_ingest_ts"), stored_as_scd_type=1)


# ---- bronze_rosters : one row per (season, roster_id) ----
@dlt.view
def _src_rosters():
    return _with_audit(_read_endpoint("rosters", allow_overwrites=True))

dlt.create_streaming_table(
    "bronze_rosters",
    comment="Raw Sleeper 'rosters' — SCD1 latest per (season, roster_id) (idempotent on re-pull).",
    table_properties={"layer": "bronze"},
)
dlt.apply_changes(target="bronze_rosters", source="_src_rosters",
                  keys=["season", "roster_id"], sequence_by=F.col("_ingest_ts"),
                  stored_as_scd_type=1)


# ---- bronze_players_raw : ONE blob (the ~12,200-key player dict) ----
# Inferring its schema would make a 12,200-column table, so land each file as one
# binary blob and parse the map in Silver. SCD1 on a constant key keeps only the
# latest pull of the (frequently-updated) player dictionary.
@dlt.view
def _src_players():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")       # whole file -> one row
        .option("cloudFiles.allowOverwrites", "true")
        .load(f"{VOLUME_ROOT}/players_nfl/")
        .select(
            F.decode(F.col("content"), "utf-8").alias("players_json"),
            F.col("path").alias("_source_file"),
            F.current_timestamp().alias("_ingest_ts"),
            F.lit("players").alias("_key"),              # single logical record
        )
    )

dlt.create_streaming_table(
    "bronze_players_raw",
    comment="Raw Sleeper player dictionary as one JSON string — SCD1 latest (exploded in Silver).",
    table_properties={"layer": "bronze"},
)
dlt.apply_changes(target="bronze_players_raw", source="_src_players",
                  keys=["_key"], sequence_by=F.col("_ingest_ts"), stored_as_scd_type=1)


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
