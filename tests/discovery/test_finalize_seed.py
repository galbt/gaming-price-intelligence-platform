import pytest

from collectors.discovery.finalize_seed import merge_reviewed, to_dim_game_rows, write_seed_csv


def test_merge_reviewed_includes_matched_and_accepted_reviews():
    matched = [{"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620, "ps_store_path": "concept/10000333"}]
    needs_review = [
        {"igdb_id": 2, "canonical_name": "Dark Souls III", "coverage": "STEAM_ONLY", "steam_appid": None, "ps_store_path": None,
         "steam_candidates": [{"id": 374320, "name": "DARK SOULS III"}]},
        {"igdb_id": 3, "canonical_name": "Fake Game", "coverage": "STEAM_ONLY", "steam_appid": None, "ps_store_path": None, "steam_candidates": []},
    ]
    review_decisions = {2: {"steam_appid": 374320}, 3: None}

    result = merge_reviewed(matched, needs_review, review_decisions)

    assert result == [
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620, "ps_store_path": "concept/10000333"},
        {"igdb_id": 2, "canonical_name": "Dark Souls III", "coverage": "STEAM_ONLY", "steam_appid": 374320, "ps_store_path": None},
    ]


def test_merge_reviewed_applies_decision_override_to_already_matched_entry():
    # Regression test: a game matched automatically on this run may have been
    # corrected by a human on an earlier run. That correction must still win.
    matched = [{"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620, "ps_store_path": "concept/10000333"}]
    review_decisions = {1: {"steam_appid": 999999}}

    result = merge_reviewed(matched, [], review_decisions)

    assert result == [
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 999999, "ps_store_path": "concept/10000333"},
    ]


def test_merge_reviewed_excludes_already_matched_entry_rejected_by_human():
    # Regression test: a game auto-matched on this run may have been
    # explicitly rejected by a human on an earlier run. The rejection must
    # still exclude it, not be silently overridden by the fresh auto-match.
    matched = [{"igdb_id": 1, "canonical_name": "Fake Game", "coverage": "BOTH", "steam_appid": 1, "ps_store_path": "concept/1"}]
    review_decisions = {1: None}

    result = merge_reviewed(matched, [], review_decisions)

    assert result == []


def test_merge_reviewed_excludes_undecided_reviews():
    matched = []
    needs_review = [{"igdb_id": 5, "canonical_name": "Pending Game", "coverage": "BOTH", "steam_appid": None, "ps_store_path": None, "steam_candidates": []}]

    result = merge_reviewed(matched, needs_review, review_decisions={})

    assert result == []


def test_merge_reviewed_applies_canonical_name_correction_for_collisions():
    matched = []
    needs_review = [
        {"igdb_id": 132181, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "steam_appid": 2050650, "ps_store_path": "concept/10004473", "steam_candidates": []},
    ]
    review_decisions = {132181: {"canonical_name": "Resident Evil 4 (2023)"}}

    result = merge_reviewed(matched, needs_review, review_decisions)

    assert result == [
        {"igdb_id": 132181, "canonical_name": "Resident Evil 4 (2023)", "coverage": "BOTH", "steam_appid": 2050650, "ps_store_path": "concept/10004473"},
    ]


def test_to_dim_game_rows_builds_game_key():
    merged = [{"igdb_id": 1, "canonical_name": "Elden Ring: Deluxe Edition", "coverage": "BOTH", "steam_appid": 1245620, "ps_store_path": "concept/10000333"}]

    rows = to_dim_game_rows(merged, added_date="2026-11-05")

    assert rows == [{
        "game_key": "elden_ring_deluxe_edition",
        "canonical_name": "Elden Ring: Deluxe Edition",
        "coverage": "BOTH",
        "steam_appid": 1245620,
        "ps_store_path": "concept/10000333",
        "igdb_id": 1,
        "added_date": "2026-11-05",
    }]


def test_to_dim_game_rows_nulls_identifiers_coverage_does_not_need():
    # Regression test: the spec requires steam_appid NULL for PS5_ONLY and
    # ps_store_path NULL for STEAM_ONLY, regardless of what's in the merged
    # dict (a misclassified or stale entry could otherwise carry both).
    merged = [{"igdb_id": 9, "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY", "steam_appid": 12345, "ps_store_path": "concept/999"}]

    rows = to_dim_game_rows(merged, added_date="2026-11-05")

    assert rows == [{
        "game_key": "a_ps5_exclusive",
        "canonical_name": "A PS5 Exclusive",
        "coverage": "PS5_ONLY",
        "steam_appid": None,
        "ps_store_path": "concept/999",
        "igdb_id": 9,
        "added_date": "2026-11-05",
    }]


def test_to_dim_game_rows_raises_on_duplicate_game_key():
    # Regression test for the Resident Evil 4 finding, reopened after review:
    # if a reviewer accepts two distinct igdb_ids that still slugify to the
    # same game_key, that must be caught, not silently written as duplicates.
    merged = [
        {"igdb_id": 20065, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "steam_appid": 1, "ps_store_path": "concept/1"},
        {"igdb_id": 132181, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "steam_appid": 2, "ps_store_path": "concept/2"},
    ]

    with pytest.raises(ValueError):
        to_dim_game_rows(merged, added_date="2026-11-05")


def test_to_dim_game_rows_raises_on_empty_game_key():
    merged = [{"igdb_id": 1, "canonical_name": "!!!", "coverage": "BOTH", "steam_appid": 1, "ps_store_path": "concept/1"}]

    with pytest.raises(ValueError):
        to_dim_game_rows(merged, added_date="2026-11-05")


def test_write_seed_csv_writes_expected_header_and_empty_nulls(tmp_path):
    rows = [{
        "game_key": "a_ps5_exclusive", "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY",
        "steam_appid": None, "ps_store_path": "concept/999", "igdb_id": 4, "added_date": "2026-11-05",
    }]
    path = tmp_path / "dim_game_seed.csv"

    write_seed_csv(rows, str(path))

    content = path.read_text()
    assert content.splitlines()[0] == "game_key,canonical_name,coverage,steam_appid,ps_store_path,igdb_id,added_date"
    assert "a_ps5_exclusive,A PS5 Exclusive,PS5_ONLY,,concept/999,4,2026-11-05" in content
