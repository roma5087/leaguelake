#!/usr/bin/env python3
"""
LeagueLake — export the what-if scoring-simulator payload for the portfolio app.

Pulls per-team-week scenario data + names + actual champions from Databricks
(Gold), reads the current & proposed scoring settings from the local raw files,
and writes a compact JSON the client-side app re-scores as sliders move.

  points_new(team,week) = non_def_points + Σ def_stats[k] × def_settings[k]

Output: app_data/leaguelake.json  (also the file the MR_002 page consumes).
"""
import subprocess, json, os, glob

PROFILE = "leaguelake"
CAT = "workspace.leaguelake"
RAW = os.path.expanduser("~/leaguelake/raw")
OUT = os.path.expanduser("~/leaguelake/app_data/leaguelake.json")

# defensive scoring keys the simulator exposes (the ones a DEF change touches)
DEF_KEYS = ["sack", "int", "ff", "fum_rec", "fum_rec_td", "def_td", "def_st_td",
            "def_st_ff", "def_st_fum_rec", "def_2pt", "safe", "blk_kick", "pts_allow",
            "pts_allow_0", "pts_allow_1_6", "pts_allow_7_13", "pts_allow_14_20",
            "pts_allow_21_27", "pts_allow_28_34", "pts_allow_35p", "def_4_and_stop",
            "tkl_loss", "st_ff", "st_fum_rec", "st_td"]


def _dbx(args, payload=None):
    cmd = ["databricks"] + args + ["--profile", PROFILE]
    if payload is not None:
        cmd += ["--json", json.dumps(payload)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return json.loads(r.stdout) if r.stdout.strip() else None


def query(sql):
    import time
    wid = _dbx(["warehouses", "list", "-o", "json"])[0]["id"]
    r = _dbx(["api", "post", "/api/2.0/sql/statements"],
             {"warehouse_id": wid, "statement": sql, "wait_timeout": "50s",
              "format": "JSON_ARRAY", "disposition": "INLINE"})
    sid, st = r["statement_id"], r["status"]["state"]
    for _ in range(40):
        if st in ("SUCCEEDED", "FAILED", "CANCELED"):
            break
        time.sleep(4)
        r = _dbx(["api", "get", f"/api/2.0/sql/statements/{sid}"]); st = r["status"]["state"]
    if st != "SUCCEEDED":
        raise RuntimeError(f"query failed: {r.get('status')}")
    return r["result"].get("data_array", [])


def _settings_from_gold():
    """season -> {'scoring': {...}, 'pws': int} from gold_scoring_settings.

    Gold is the source of truth. Returns {} (and warns) if the table isn't there
    yet — a migration bridge so a pre-deploy run still works off the raw files.
    Once the pipeline with gold_scoring_settings has run, the fallback is dead.
    """
    out = {}
    try:
        for season, scoring_json, pws in query(
                f"SELECT season, scoring_json, playoff_week_start FROM {CAT}.gold_scoring_settings"):
            out[season] = {"scoring": json.loads(scoring_json) if scoring_json else {},
                           "pws": int(pws) if pws is not None else None}
    except Exception as e:
        print(f"  WARN: gold_scoring_settings unavailable ({e}); falling back to raw league files")
    return out


_SETTINGS = None  # lazily loaded once (see main)


def _raw_league(season):
    return json.load(open(f"{RAW}/season={season}/league/league.json"))


def _scoring(season):
    """Full scoring_settings dict for a season — Gold-preferred, raw fallback."""
    g = (_SETTINGS or {}).get(season)
    if g and g["scoring"]:
        return g["scoring"]
    return _raw_league(season)["scoring_settings"]


def playoff_week_start(season):
    g = (_SETTINGS or {}).get(season)
    if g and g["pws"] is not None:
        return g["pws"]
    return _raw_league(season)["settings"]["playoff_week_start"]


def def_settings(season):
    """DEF-relevant scoring settings for a season (from Gold, raw as fallback)."""
    sc = _scoring(season)
    return {k: sc[k] for k in DEF_KEYS if k in sc and sc[k] is not None}


def main():
    global _SETTINGS
    _SETTINGS = _settings_from_gold()  # Gold is the source of truth for scoring config

    # managers: user_id -> current display_name
    managers = {u: n for u, n in query(
        f"SELECT user_id, display_name FROM {CAT}.dim_manager WHERE `__END_AT` IS NULL")}

    # actual champion per season (winner of the p=1 bracket node -> user_id)
    champ = {s: u for s, u in query(f"""
        SELECT b.season, r.owner_id
        FROM {CAT}.bronze_winners_bracket b
        JOIN {CAT}.bronze_rosters r ON b.season=r.season AND b.w=r.roster_id
        WHERE b.p = 1""")}

    # per-team-week scenario rows
    rows = query(f"""
        SELECT season, week, user_id, matchup_id, actual_points, non_def_points,
               def_id, to_json(def_stats) AS def_stats, is_regular
        FROM {CAT}.gold_scenario_input ORDER BY season, week""")

    seasons = {}
    for season, week, uid, mid, actual, non_def, did, dstats, is_reg in rows:
        s = seasons.setdefault(season, {
            "playoff_week_start": playoff_week_start(season),
            "actual_champion": managers.get(champ.get(season), champ.get(season)),
            "def_settings_current": def_settings(season),
            "team_weeks": [],
        })
        s["team_weeks"].append({
            "week": int(week), "user_id": uid, "matchup_id": int(mid) if mid is not None else None,
            "actual": float(actual), "non_def": float(non_def), "def_id": did,
            "def_stats": {k: v for k, v in (json.loads(dstats) if dstats else {}).items() if k in DEF_KEYS},
            "reg": (is_reg in (True, "true", 1)),
        })

    # NFL defense value board — each real defense's season-summed stats
    for season, did, dstats in query(
            f"SELECT season, def_id, to_json(stats) FROM {CAT}.gold_nfl_defense_stats"):
        if season in seasons:
            seasons[season].setdefault("defenses", {})[did] = json.loads(dstats)

    payload = {
        "generated_season_scope": sorted(seasons),
        "managers": managers,
        "def_settings_proposed_2026": def_settings("2026"),
        "seasons": seasons,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(payload, open(OUT, "w"), separators=(",", ":"))
    print(f"wrote {OUT}  ({os.path.getsize(OUT)/1024:.0f} KB)")
    print(f"  seasons: {sorted(seasons)} | managers: {len(managers)} | "
          f"team-weeks: {sum(len(v['team_weeks']) for v in seasons.values())}")
    print(f"  actual champions: {{{', '.join(f'{s}:{v['actual_champion']}' for s,v in sorted(seasons.items()))}}}")


if __name__ == "__main__":
    main()
