#!/usr/bin/env python3
"""
LeagueLake — push the collector's raw JSON into the Unity Catalog Volume.

Databricks Free Edition can't reach the internet, so the collector runs locally
and lands raw files under ./raw. This is the handoff: it uploads ./raw into the
Volume the Bronze pipeline reads, then VERIFIES the Volume file counts match what
was produced locally (per season, cross-checked against the collector's
_manifest.json). Without this check a partial/failed upload would silently feed
stale or missing data into Bronze.

  python -m collector.push_to_volume            # uses the `leaguelake` CLI profile
  LEAGUELAKE_PROFILE=other python collector/push_to_volume.py

Exit 0 = uploaded and verified; 1 = a mismatch (do not trust Bronze until fixed).

NOTE: the mutable-dimension Bronze tables (league/rosters/players) are configured
with cloudFiles.allowOverwrites + SCD1 apply_changes, so re-running this to push
an updated pull is idempotent — a changed file is re-ingested and collapses to the
latest row instead of going stale or double-counting.
"""
import json
import os
import subprocess
import sys

PROFILE = os.environ.get("LEAGUELAKE_PROFILE", "leaguelake")
LOCAL_RAW = os.path.expanduser("~/leaguelake/raw")
VOLUME_RAW = "dbfs:/Volumes/workspace/leaguelake/bronze/raw"


def _run(args):
    cmd = ["databricks"] + args + (["--profile", PROFILE] if PROFILE else [])
    return subprocess.run(cmd, capture_output=True, text=True)


def _ls(path):
    """List one Volume directory; returns [(name, is_dir), ...] ([] if missing)."""
    r = _run(["fs", "ls", path, "-o", "json"])
    if r.returncode != 0:
        return []
    return [(e["name"], e["is_directory"]) for e in json.loads(r.stdout or "[]")]


def _count_volume_files(path):
    """Recursively count non-directory files under a Volume path."""
    total = 0
    for name, is_dir in _ls(path):
        child = f"{path}/{name}"
        total += _count_volume_files(child) if is_dir else 1
    return total


def _local_file_count():
    return sum(len(files) for _, _, files in os.walk(LOCAL_RAW))


def main():
    if not os.path.isdir(LOCAL_RAW):
        print(f"ERROR: {LOCAL_RAW} not found — run the collector first.")
        return 1

    local_total = _local_file_count()
    print(f"Uploading {local_total} files: {LOCAL_RAW} -> {VOLUME_RAW}")
    cp = _run(["fs", "cp", "-r", LOCAL_RAW, VOLUME_RAW, "--overwrite"])
    if cp.returncode != 0:
        print(f"ERROR: upload failed: {cp.stderr.strip()}")
        return 1

    volume_total = _count_volume_files(VOLUME_RAW)
    ok = volume_total == local_total
    print(f"  local files:  {local_total}")
    print(f"  volume files: {volume_total}   {'OK' if ok else 'MISMATCH'}")

    # cross-check per-season against the collector's manifest, if present
    man_path = os.path.join(LOCAL_RAW, "_manifest.json")
    if os.path.exists(man_path):
        man = json.load(open(man_path))
        print("  per-season (manifest 'files' vs volume):")
        for s in man.get("seasons", []):
            season, expected = s["season"], s.get("files")
            got = _count_volume_files(f"{VOLUME_RAW}/season={season}")
            mark = "OK" if got == expected else "MISMATCH"
            ok = ok and got == expected
            print(f"    season={season}: manifest={expected} volume={got}  {mark}")

    print("PASS — Volume matches local." if ok else "FAIL — Volume does not match local.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
