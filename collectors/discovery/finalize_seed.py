import csv

from collectors.discovery.build_seed import game_key_for

SEED_FIELDNAMES = ["game_key", "canonical_name", "coverage", "steam_appid", "ps_store_path", "igdb_id", "added_date"]


def merge_reviewed(matched: list[dict], needs_review: list[dict], review_decisions: dict[int, dict | None]) -> list[dict]:
    result = [dict(entry) for entry in matched]
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
    for entry in merged:
        rows.append(
            {
                "game_key": game_key_for(entry["canonical_name"]),
                "canonical_name": entry["canonical_name"],
                "coverage": entry["coverage"],
                "steam_appid": entry["steam_appid"],
                "ps_store_path": entry["ps_store_path"],
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
