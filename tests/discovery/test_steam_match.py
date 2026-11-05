import responses

from collectors.discovery.steam_match import normalize_name, search_steam, best_match


def test_normalize_name_strips_edition_suffix():
    assert normalize_name("Elden Ring: Deluxe Edition") == "elden ring"


def test_normalize_name_lowercases_and_trims():
    assert normalize_name("  Hollow Knight  ") == "hollow knight"


def test_normalize_name_leaves_sequel_numbers_alone():
    assert normalize_name("Dark Souls III") == "dark souls iii"


@responses.activate
def test_search_steam_returns_items():
    responses.add(
        responses.GET,
        "https://store.steampowered.com/api/storesearch/",
        json={"total": 1, "items": [{"id": 1245620, "name": "Elden Ring"}]},
        status=200,
    )

    results = search_steam("Elden Ring")

    assert results == [{"id": 1245620, "name": "Elden Ring"}]
    assert responses.calls[0].request.params["term"] == "Elden Ring"


@responses.activate
def test_search_steam_returns_empty_list_when_no_results():
    responses.add(
        responses.GET,
        "https://store.steampowered.com/api/storesearch/",
        json={"total": 0, "items": []},
        status=200,
    )

    assert search_steam("Definitely Not A Real Game Title Xyz") == []


def test_best_match_high_confidence_on_exact_normalized_name():
    results = [{"id": 1245620, "name": "Elden Ring"}]
    match, confident = best_match("Elden Ring", results)
    assert match == {"id": 1245620, "name": "Elden Ring"}
    assert confident is True


def test_best_match_low_confidence_on_sequel_mismatch():
    results = [{"id": 1, "name": "Dark Souls"}]
    match, confident = best_match("Dark Souls III", results)
    assert match == {"id": 1, "name": "Dark Souls"}
    assert confident is False


def test_best_match_none_when_no_results():
    match, confident = best_match("Some Game", [])
    assert match is None
    assert confident is False
