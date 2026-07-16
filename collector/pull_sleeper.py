#!/usr/bin/env python3
"""
LeagueLake — Sleeper collector (Bronze ingestion).

Pulls a Sleeper league's full history (all seasons via the previous_league_id
chain) and lands raw, unmodified JSON on disk in a partitioned layout that
Databricks Auto Loader can incrementally ingest later.

Read-only: every call is a GET against Sleeper's public API. Nothing is written
back to the league.

Layout produced:
    raw/
      _manifest.json                      # run metadata: timestamp, seasons, counts
      players_nfl/players_nfl.json        # global player dictionary (pulled once)
      season=<YYYY>/league/league.json
      season=<YYYY>/users/users.json
      season=<YYYY>/rosters/rosters.json
      season=<YYYY>/matchups/week=<W>.json
      season=<YYYY>/transactions/week=<W>.json
      season=<YYYY>/drafts/drafts.json
      season=<YYYY>/draft_picks/draft=<id>.json
      season=<YYYY>/winners_bracket/winners_bracket.json
      season=<YYYY>/losers_bracket/losers_bracket.json
"""
from __future__ import annotations
import argparse, json, os, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone

BASE = "https://api.sleeper.app/v1"
MAX_WEEK = 18          # loop weeks 1..18; empties are skipped
POLITE_SLEEP = 0.05    # small courtesy delay between calls


def get(path: str, *, allow_404: bool = False, retries: int = 4):
    """GET {BASE}{path} -> parsed JSON. Retries transient errors (429/5xx/network)
    with exponential backoff. Returns None on 404 when allowed."""
    url = f"{BASE}{path}"
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
            time.sleep(POLITE_SLEEP)
            return data
        except urllib.error.HTTPError as e:
            if e.code == 404 and allow_404:
                return None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2 ** attempt)   # backoff: 1, 2, 4, 8s
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise


def write_json(path: str, obj) -> int:
    """Write JSON atomically (temp file + rename) so a crash/interrupt can't
    leave a truncated file at a real path that Auto Loader would then ingest."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)   # atomic rename on the same filesystem
    return os.path.getsize(path)


def season_chain(current_league_id: str) -> list[dict]:
    """Walk previous_league_id backwards; return leagues newest-first.
    Guards against cycles (visited set) and a broken/deleted prior league id (404)."""
    chain, cur, seen = [], current_league_id, set()
    while cur and cur not in ("0", "") and cur not in seen:
        seen.add(cur)
        lg = get(f"/league/{cur}", allow_404=True)
        if lg is None:            # a prior-season league id that no longer resolves
            break
        chain.append(lg)
        cur = lg.get("previous_league_id")
    return chain


def pull_season(lg: dict, out_root: str) -> dict:
    season = lg["season"]
    lid = lg["league_id"]
    base = os.path.join(out_root, f"season={season}")
    counts = {"season": season, "league_id": lid, "name": lg.get("name"),
              "status": lg.get("status"), "files": 0, "matchup_weeks": 0,
              "transaction_weeks": 0, "transactions": 0, "draft_picks": 0}

    write_json(os.path.join(base, "league", "league.json"), lg); counts["files"] += 1
    write_json(os.path.join(base, "users", "users.json"),
               get(f"/league/{lid}/users")); counts["files"] += 1
    write_json(os.path.join(base, "rosters", "rosters.json"),
               get(f"/league/{lid}/rosters")); counts["files"] += 1

    # Weekly matchups + transactions (skip empty weeks)
    for wk in range(1, MAX_WEEK + 1):
        mk = get(f"/league/{lid}/matchups/{wk}", allow_404=True)
        if mk:
            write_json(os.path.join(base, "matchups", f"week={wk}.json"), mk)
            counts["files"] += 1; counts["matchup_weeks"] += 1
        tx = get(f"/league/{lid}/transactions/{wk}", allow_404=True)
        if tx:
            write_json(os.path.join(base, "transactions", f"week={wk}.json"), tx)
            counts["files"] += 1; counts["transaction_weeks"] += 1
            counts["transactions"] += len(tx)

    # Raw weekly player stats (NFL-wide per season/week) — powers what-if re-scoring.
    # Only for seasons that have been played (skip the pre-draft upcoming season).
    if lg.get("status") != "pre_draft":
        for wk in range(1, MAX_WEEK + 1):
            st = get(f"/stats/nfl/regular/{season}/{wk}", allow_404=True)
            if st:
                write_json(os.path.join(base, "player_stats", f"week={wk}.json"), st)
                counts["files"] += 1; counts["stat_weeks"] = counts.get("stat_weeks", 0) + 1

    # Drafts + picks
    drafts = get(f"/league/{lid}/drafts", allow_404=True) or []
    write_json(os.path.join(base, "drafts", "drafts.json"), drafts); counts["files"] += 1
    for d in drafts:
        picks = get(f"/draft/{d['draft_id']}/picks", allow_404=True) or []
        write_json(os.path.join(base, "draft_picks", f"draft={d['draft_id']}.json"), picks)
        counts["files"] += 1; counts["draft_picks"] += len(picks)

    # Playoff brackets
    for bracket in ("winners_bracket", "losers_bracket"):
        b = get(f"/league/{lid}/{bracket}", allow_404=True)
        if b is not None:
            write_json(os.path.join(base, bracket, f"{bracket}.json"), b)
            counts["files"] += 1

    return counts


def main():
    ap = argparse.ArgumentParser(description="Pull a Sleeper league's full history to raw Bronze files.")
    ap.add_argument("--league-id", default="1367152252250247168",
                    help="Current-season league ID (chains back through prior seasons).")
    ap.add_argument("--out", default=os.path.expanduser("~/leaguelake/raw"),
                    help="Output root directory.")
    ap.add_argument("--skip-players", action="store_true",
                    help="Skip the large global player dictionary.")
    args = ap.parse_args()

    started = datetime.now(timezone.utc).isoformat()
    print(f"[{started}] LeagueLake collector — league {args.league_id}")
    print(f"  output: {args.out}")

    chain = season_chain(args.league_id)
    print(f"  seasons found: {[lg['season'] for lg in chain]}")

    # Global player dictionary — pulled once (shared across seasons)
    if not args.skip_players:
        print("  pulling player dictionary (~5MB)...")
        sz = write_json(os.path.join(args.out, "players_nfl", "players_nfl.json"),
                        get("/players/nfl"))
        print(f"    players_nfl.json ({sz/1e6:.1f} MB)")

    all_counts = []
    for lg in chain:
        c = pull_season(lg, args.out)
        all_counts.append(c)
        print(f"  season {c['season']} ({c['name']}, {c['status']}): "
              f"{c['files']} files, {c['matchup_weeks']} matchup wks, "
              f"{c['transactions']} tx, {c['draft_picks']} draft picks")

    manifest = {
        "collector": "leaguelake",
        "run_started_utc": started,
        "run_finished_utc": datetime.now(timezone.utc).isoformat(),
        "current_league_id": args.league_id,
        "source": BASE,
        "seasons": all_counts,
    }
    write_json(os.path.join(args.out, "_manifest.json"), manifest)
    print(f"[done] wrote manifest with {len(all_counts)} seasons.")


if __name__ == "__main__":
    sys.exit(main())
