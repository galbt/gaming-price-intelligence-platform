import csv
import re

from collectors.discovery.steam_match import search_steam, best_match

STEAM_APPID_RE = re.compile(r"store\.steampowered\.com/app/(\d+)")
PS_STORE_PATH_RE = re.compile(r"store\.playstation\.com/[^/]+/((?:concept|product)/[^/?]+)")


def game_key_for(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def extract_steam_appid(websites: list[str]) -> int | None:
    for url in websites:
        match = STEAM_APPID_RE.search(url)
        if match:
            return int(match.group(1))
    return None


def extract_ps_store_path(websites: list[str]) -> str | None:
    for url in websites:
        match = PS_STORE_PATH_RE.search(url)
        if match:
            return match.group(1)
    return None


def classify_coverage(has_pc: bool, has_ps5: bool) -> str:
    if has_pc and has_ps5:
        return "BOTH"
    if has_pc:
        return "STEAM_ONLY"
    if has_ps5:
        return "PS5_ONLY"
    return "OTHER"


def build_candidates(pc_games: list[dict], ps5_games: list[dict]) -> list[dict]:
    pc_ids = {g["id"] for g in pc_games}
    ps5_ids = {g["id"] for g in ps5_games}

    by_id: dict[int, dict] = {}
    for game in pc_games + ps5_games:
        by_id.setdefault(game["id"], game)

    candidates = []
    for game_id, game in by_id.items():
        has_pc = game_id in pc_ids
        has_ps5 = game_id in ps5_ids or extract_ps_store_path(game["websites"]) is not None
        coverage = classify_coverage(has_pc, has_ps5)
        if coverage == "OTHER":
            continue
        candidates.append(
            {
                "igdb_id": game["id"],
                "canonical_name": game["name"],
                "coverage": coverage,
                "websites": game["websites"],
            }
        )
    return candidates


def find_game_key_collisions(candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    groups: dict[str, list[dict]] = {}
    for candidate in candidates:
        key = game_key_for(candidate["canonical_name"])
        groups.setdefault(key, []).append(candidate)

    clean = []
    collided = []
    for candidate in candidates:
        key = game_key_for(candidate["canonical_name"])
        group = groups[key]
        distinct_ids = {c["igdb_id"] for c in group}
        if len(distinct_ids) > 1:
            collided.append(candidate)
        else:
            clean.append(candidate)
    return clean, collided


def attach_identifiers(candidates: list[dict]) -> list[dict]:
    result = []
    for candidate in candidates:
        updated = {k: v for k, v in candidate.items() if k != "websites"}
        updated["steam_appid"] = extract_steam_appid(candidate["websites"])
        updated["ps_store_path"] = extract_ps_store_path(candidate["websites"])
        result.append(updated)
    return result


def match_candidates(candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    matched = []
    needs_review = []
    for original in candidates:
        candidate = dict(original)
        steam_candidates_found: list[dict] = []

        needs_steam = candidate["coverage"] in ("BOTH", "STEAM_ONLY") and candidate["steam_appid"] is None
        if needs_steam:
            steam_candidates_found = search_steam(candidate["canonical_name"])
            match, confident = best_match(candidate["canonical_name"], steam_candidates_found)
            if confident:
                candidate["steam_appid"] = match["id"]

        missing_steam = candidate["coverage"] in ("BOTH", "STEAM_ONLY") and candidate["steam_appid"] is None
        missing_ps = candidate["coverage"] in ("BOTH", "PS5_ONLY") and candidate["ps_store_path"] is None

        if missing_steam or missing_ps:
            candidate["steam_candidates"] = steam_candidates_found
            needs_review.append(candidate)
        else:
            matched.append(candidate)
    return matched, needs_review


def write_candidates_csv(candidates: list[dict], path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["igdb_id", "canonical_name", "coverage"])
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(
                {k: candidate[k] for k in ("igdb_id", "canonical_name", "coverage")}
            )
