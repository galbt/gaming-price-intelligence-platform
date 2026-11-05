from collectors.discovery.build_seed import (
    game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage, build_candidates,
    find_game_key_collisions, write_candidates_csv, attach_identifiers, match_candidates,
)
import csv
from unittest.mock import patch


def test_game_key_for_slugifies_name():
    assert game_key_for("Elden Ring") == "elden_ring"


def test_game_key_for_strips_punctuation_and_collapses_runs():
    assert game_key_for("Elden Ring: Deluxe Edition!") == "elden_ring_deluxe_edition"


def test_extract_steam_appid_finds_appid_in_url_list():
    urls = ["https://www.rockstargames.com/gta-v", "https://store.steampowered.com/app/1245620/Elden_Ring/"]
    assert extract_steam_appid(urls) == 1245620


def test_extract_steam_appid_none_when_absent():
    assert extract_steam_appid(["https://en.wikipedia.org/wiki/Elden_Ring"]) is None


def test_extract_ps_store_path_finds_concept_path():
    urls = ["https://store.playstation.com/en-us/concept/10000333", "https://store.steampowered.com/app/1245620"]
    assert extract_ps_store_path(urls) == "concept/10000333"


def test_extract_ps_store_path_finds_product_path():
    urls = ["https://store.playstation.com/en-gb/product/EP1004-PPSA01721_00-GTAVCROSSGENBUND"]
    assert extract_ps_store_path(urls) == "product/EP1004-PPSA01721_00-GTAVCROSSGENBUND"


def test_extract_ps_store_path_none_when_absent():
    assert extract_ps_store_path(["https://store.steampowered.com/app/1245620"]) is None


def test_classify_coverage_both():
    assert classify_coverage(has_pc=True, has_ps5=True) == "BOTH"


def test_classify_coverage_steam_only():
    assert classify_coverage(has_pc=True, has_ps5=False) == "STEAM_ONLY"


def test_classify_coverage_ps5_only():
    assert classify_coverage(has_pc=False, has_ps5=True) == "PS5_ONLY"


def test_classify_coverage_other():
    assert classify_coverage(has_pc=False, has_ps5=False) == "OTHER"


def test_build_candidates_classifies_and_discards_other():
    pc_games = [
        {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": ["https://store.steampowered.com/app/1245620"]},
        {"id": 3, "name": "A PC Exclusive", "platforms": [6], "websites": []},
    ]
    ps5_games = [
        {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": ["https://store.steampowered.com/app/1245620"]},
        {"id": 4, "name": "A PS5 Exclusive", "platforms": [167], "websites": []},
    ]

    result = build_candidates(pc_games, ps5_games, pc_id=6, ps5_id=167)

    assert result == [
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "websites": ["https://store.steampowered.com/app/1245620"]},
        {"igdb_id": 3, "canonical_name": "A PC Exclusive", "coverage": "STEAM_ONLY", "websites": []},
        {"igdb_id": 4, "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY", "websites": []},
    ]


def test_build_candidates_counts_ps_url_as_ps5_even_when_platforms_field_misses_it():
    # Regression test for the Red Dead Redemption 2 finding: a game can have a
    # live, resolvable PS Store URL without "PlayStation 5" in its platforms list.
    pc_games = [
        {"id": 2, "name": "Red Dead Redemption 2", "platforms": [6], "websites": ["https://store.playstation.com/en-us/concept/209441"]},
    ]
    ps5_games = []  # does NOT appear in the PS5-platform query

    result = build_candidates(pc_games, ps5_games, pc_id=6, ps5_id=167)

    assert result == [
        {
            "igdb_id": 2,
            "canonical_name": "Red Dead Redemption 2",
            "coverage": "BOTH",
            "websites": ["https://store.playstation.com/en-us/concept/209441"],
        },
    ]


def test_build_candidates_trusts_own_platforms_field_when_missing_from_query_page():
    # Regression test: IGDB's platform-filtered queries are paginated and capped
    # (max_pages), with no stable sort, so a real game can fall outside the
    # fetched page window and never appear in one query's result list at all.
    # Its own `platforms` field must still be trusted, not just query membership.
    pc_games = []  # never appeared in the (capped, unsorted) PC-platform query
    ps5_games = [
        {"id": 5, "name": "Paginated Mutual Game", "platforms": [6, 167], "websites": []},
    ]

    result = build_candidates(pc_games, ps5_games, pc_id=6, ps5_id=167)

    assert result == [
        {"igdb_id": 5, "canonical_name": "Paginated Mutual Game", "coverage": "BOTH", "websites": []},
    ]


def test_find_game_key_collisions_separates_duplicate_titles():
    # Regression test for the Resident Evil 4 finding: two distinct IGDB
    # entries (2005 original, 2023 remake) sharing the identical title.
    candidates = [
        {"igdb_id": 20065, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "websites": []},
        {"igdb_id": 132181, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "websites": []},
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "websites": []},
    ]

    clean, collided = find_game_key_collisions(candidates)

    assert clean == [{"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "websites": []}]
    assert collided == [
        {"igdb_id": 20065, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "websites": []},
        {"igdb_id": 132181, "canonical_name": "Resident Evil 4", "coverage": "BOTH", "websites": []},
    ]


def test_write_candidates_csv_round_trips_commas_in_titles(tmp_path):
    candidates = [
        {"igdb_id": 1, "canonical_name": "Tom Clancy's, The Division", "coverage": "BOTH", "websites": []},
    ]
    path = tmp_path / "candidates.csv"

    write_candidates_csv(candidates, str(path))

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [{"igdb_id": "1", "canonical_name": "Tom Clancy's, The Division", "coverage": "BOTH"}]


def test_attach_identifiers_resolves_from_websites_and_nulls_inapplicable_fields():
    candidates = [
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH",
         "websites": ["https://store.steampowered.com/app/1245620", "https://store.playstation.com/en-us/concept/10000333"]},
        {"igdb_id": 2, "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY", "websites": []},
    ]

    result = attach_identifiers(candidates)

    assert result == [
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620, "ps_store_path": "concept/10000333"},
        {"igdb_id": 2, "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY", "steam_appid": None, "ps_store_path": None},
    ]


def test_match_candidates_passes_through_fully_resolved_without_steam_call():
    candidates = [
        {"igdb_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620, "ps_store_path": "concept/10000333"},
    ]

    with patch("collectors.discovery.build_seed.search_steam") as mock_search:
        matched, needs_review = match_candidates(candidates)

    mock_search.assert_not_called()
    assert matched == candidates
    assert needs_review == []


def test_match_candidates_falls_back_to_steam_search_when_appid_missing():
    candidates = [
        {"igdb_id": 2, "canonical_name": "Marvel's Spider-Man Remastered", "coverage": "BOTH", "steam_appid": None, "ps_store_path": "concept/10000762"},
    ]

    with patch("collectors.discovery.build_seed.search_steam", return_value=[{"id": 1817070, "name": "Marvel's Spider-Man Remastered"}]):
        matched, needs_review = match_candidates(candidates)

    assert matched == [
        {"igdb_id": 2, "canonical_name": "Marvel's Spider-Man Remastered", "coverage": "BOTH", "steam_appid": 1817070, "ps_store_path": "concept/10000762"},
    ]
    assert needs_review == []


def test_match_candidates_sends_unresolved_ps_store_path_to_review():
    candidates = [
        {"igdb_id": 3, "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY", "steam_appid": None, "ps_store_path": None},
    ]

    matched, needs_review = match_candidates(candidates)

    assert matched == []
    assert needs_review == [
        {"igdb_id": 3, "canonical_name": "A PS5 Exclusive", "coverage": "PS5_ONLY", "steam_appid": None, "ps_store_path": None, "steam_candidates": []},
    ]


def test_match_candidates_sends_low_confidence_steam_fallback_to_review():
    candidates = [
        {"igdb_id": 4, "canonical_name": "Dark Souls III", "coverage": "STEAM_ONLY", "steam_appid": None, "ps_store_path": None},
    ]

    with patch("collectors.discovery.build_seed.search_steam", return_value=[{"id": 999, "name": "Dark Souls"}]):
        matched, needs_review = match_candidates(candidates)

    assert matched == []
    assert needs_review == [
        {"igdb_id": 4, "canonical_name": "Dark Souls III", "coverage": "STEAM_ONLY", "steam_appid": None, "ps_store_path": None,
         "steam_candidates": [{"id": 999, "name": "Dark Souls"}]},
    ]
