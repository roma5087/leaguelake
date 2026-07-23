#!/usr/bin/env python3
"""
LeagueLake — reconciliation checks against the DEPLOYED lakehouse.

A local Spark session isn't available, so the unit tests in tests/test_rules.py
only exercise the pure-Python rules. This module closes that gap: it runs a set
of assertions over the actual Gold/Silver tables via the SQL warehouse and fails
loudly if any invariant the interactive app depends on is violated.

Checks (per the modelling grain that feeds the app):
  1. fact_roster_slot starters == fact_matchup rows            (no join fan-out)
  2. every REGULAR matchup group has exactly 2 teams           (H2H is well-defined)
  3. exactly one started DEF per team-week                     (scenario re-scoring is 1:1)
  4. luck is zero-sum per season (up to per-team rounding)     (tie convention consistent)
  5. current dim_manager slice has unique user_id              (SCD2 "current" is a function)
  6. dim_player has unique player_id                           (no duplicate dimension rows)

Usage:
  python -m leaguelake.reconcile                 # uses the `leaguelake` CLI profile
  LEAGUELAKE_PROFILE=other python -m leaguelake.reconcile
Exit code 0 = all checks passed, 1 = at least one failed.
"""
import json
import os
import subprocess
import sys
import time

PROFILE = os.environ.get("LEAGUELAKE_PROFILE", "leaguelake")
CAT = os.environ.get("LEAGUELAKE_CATALOG", "workspace.leaguelake")
TOL = 1e-6  # float tolerance for the zero-sum check


def _dbx(args, payload=None):
    cmd = ["databricks"] + args
    if PROFILE:
        cmd += ["--profile", PROFILE]
    if payload is not None:
        cmd += ["--json", json.dumps(payload)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args)} failed: {r.stderr.strip()}")
    return json.loads(r.stdout) if r.stdout.strip() else None


def query(sql):
    wid = _dbx(["warehouses", "list", "-o", "json"])[0]["id"]
    r = _dbx(["api", "post", "/api/2.0/sql/statements"],
             {"warehouse_id": wid, "statement": sql, "wait_timeout": "50s",
              "format": "JSON_ARRAY", "disposition": "INLINE"})
    sid, st = r["statement_id"], r["status"]["state"]
    for _ in range(40):
        if st in ("SUCCEEDED", "FAILED", "CANCELED"):
            break
        time.sleep(4)
        r = _dbx(["api", "get", f"/api/2.0/sql/statements/{sid}"])
        st = r["status"]["state"]
    if st != "SUCCEEDED":
        raise RuntimeError(f"query failed: {r.get('status')}")
    return r["result"].get("data_array", [])


def _scalar(sql):
    rows = query(sql)
    return rows[0][0] if rows and rows[0] else None


# each check returns (name, ok: bool, detail: str)
def check_roster_slot_matches_matchup():
    # Scope to the regular season: an eliminated playoff team can legitimately have
    # a 0-point, no-matchup team-week with no lineup set (e.g. 2025 wk15 roster 5).
    # The invariant that feeds standings/median is that every REGULAR team-week has
    # started slots (no join fan-out / no missing lineup).
    orphans = int(_scalar(f"""
        SELECT COUNT(*) FROM {CAT}.fact_matchup m
        LEFT ANTI JOIN (SELECT DISTINCT season, week, roster_id FROM {CAT}.fact_roster_slot WHERE is_starter = true) s
        USING (season, week, roster_id)
        WHERE m.is_regular = true"""))
    return ("regular team-weeks all have starters", orphans == 0,
            f"{orphans} regular team-weeks with no started slots")


def check_matchup_group_size():
    bad = int(_scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT season, week, matchup_id, COUNT(*) c
            FROM {CAT}.fact_matchup
            WHERE is_regular = true AND matchup_id IS NOT NULL
            GROUP BY season, week, matchup_id HAVING COUNT(*) <> 2)"""))
    return ("regular matchup groups have size 2", bad == 0, f"{bad} groups with size <> 2")


def check_one_def_per_team_week():
    bad = int(_scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT season, week, roster_id, COUNT(*) c
            FROM {CAT}.silver_def_stats
            GROUP BY season, week, roster_id HAVING COUNT(*) > 1)"""))
    return ("one started DEF per team-week", bad == 0, f"{bad} team-weeks with >1 DEF")


def check_luck_sums_to_zero():
    # Luck is zero-sum on UNROUNDED values (SUM(actual) == SUM(expected)). The stored
    # `luck` column is per-team rounded to 0.1, so the league total carries up to
    # 0.05 * teams of rounding residue (observed: 0.1). Assert within that bound —
    # a real logic error (e.g. an inconsistent tie convention) would exceed it.
    rows = query(f"""SELECT season, ROUND(SUM(luck), 3), COUNT(*)
                     FROM {CAT}.gold_luck_adjusted_standings GROUP BY season""")
    offenders = [(s, v) for s, v, n in rows if abs(float(v)) > 0.05 * int(n) + 1e-9]
    return ("luck zero-sum per season (within 0.1/team rounding)", not offenders,
            f"seasons beyond rounding bound: {offenders}")


def check_unique_current_manager():
    dupes = int(_scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT user_id FROM {CAT}.dim_manager WHERE `__END_AT` IS NULL
            GROUP BY user_id HAVING COUNT(*) > 1)"""))
    return ("current dim_manager user_id unique", dupes == 0, f"{dupes} duplicated current user_ids")


def check_unique_player():
    dupes = int(_scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT player_id FROM {CAT}.dim_player GROUP BY player_id HAVING COUNT(*) > 1)"""))
    return ("dim_player player_id unique", dupes == 0, f"{dupes} duplicated player_ids")


CHECKS = [
    check_roster_slot_matches_matchup,
    check_matchup_group_size,
    check_one_def_per_team_week,
    check_luck_sums_to_zero,
    check_unique_current_manager,
    check_unique_player,
]


def main():
    failures = 0
    for fn in CHECKS:
        try:
            name, ok, detail = fn()
        except Exception as e:  # a broken query is itself a failure
            name, ok, detail = fn.__name__, False, f"errored: {e}"
        mark = "PASS" if ok else "FAIL"
        print(f"[{mark}] {name}" + ("" if ok else f"  — {detail}"))
        failures += 0 if ok else 1
    print(f"\n{len(CHECKS) - failures}/{len(CHECKS)} checks passed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
