import csv

from collectors.discovery.build_seed import game_key_for

SEED_FIELDNAMES = ["game_key", "canonical_name", "coverage", "steam_appid", "ps_store_path", "igdb_id", "added_date"]


def merge_reviewed(matched: list[dict], needs_review: list[dict], review_decisions: dict[int, dict | None]) -> list[dict]:
    result = []
    for entry in matched:
        igdb_id = entry["igdb_id"]
        if igdb_id in review_decisions:
            decision = review_decisions[igdb_id]
            if decision is None:
                continue
            updated = dict(entry)
            updated.update(decision)
            result.append(updated)
        else:
            result.append(dict(entry))
    for entry in needs_review:
        decision = review_decisions.get(entry["igdb_id"])
        if decision is None:
            continue
        updated = {k: v for k, v in entry.items() if k != "steam_candidates"}
        updated.update(decision)
        result.append(updated)
    return result


def to_dim_game_rows(merged: list[dict], added_date: str) -> list[dict]:
    rows = []
    seen_keys: dict[str, int] = {}
    for entry in merged:
        key = game_key_for(entry["canonical_name"])
        if not key:
            raise ValueError(
                f"empty game_key for igdb_id={entry['igdb_id']} (canonical_name={entry['canonical_name']!r})"
            )
        if key in seen_keys and seen_keys[key] != entry["igdb_id"]:
            raise ValueError(
                f"duplicate game_key {key!r} for igdb_id={entry['igdb_id']} and igdb_id={seen_keys[key]} "
                "— a human reviewer must disambiguate (e.g. correct canonical_name) before this can be loaded"
            )
        seen_keys[key] = entry["igdb_id"]

        coverage = entry["coverage"]
        rows.append(
            {
                "game_key": key,
                "canonical_name": entry["canonical_name"],
                "coverage": coverage,
                "steam_appid": entry["steam_appid"] if coverage in ("BOTH", "STEAM_ONLY") else None,
                "ps_store_path": entry["ps_store_path"] if coverage in ("BOTH", "PS5_ONLY") else None,
                "igdb_id": entry["igdb_id"],
                "added_date": added_date,
            }
        )
    return rows


def write_seed_csv(rows: list[dict], path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SEED_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
