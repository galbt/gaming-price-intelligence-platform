# Game Discovery → dim_game (RAWG + Steam) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an API-driven game discovery pipeline that produces a hand-reviewed `dim_game` seed file for the `BOTH` and `STEAM_ONLY` coverage tiers (RAWG + Steam only — PlatPrices and the `PS5_ONLY` tier are explicitly deferred, see Scope below), and load it into an Iceberg `dim_game` table on the Plan 1 infrastructure.

**Architecture:** A local Python script queries RAWG's `/games` endpoint filtered by platform, classifies each result as `BOTH` or `STEAM_ONLY` by inspecting its own `platforms` list client-side (not relying on RAWG's filter semantics to do the intersection), verifies each candidate against Steam's storesearch endpoint, auto-accepts high-confidence name matches and routes the rest to a human-reviewed CSV, then a finalize step merges review decisions into a committed seed file. A PySpark batch job (run manually via `spark-submit` against the Plan 1 cluster — Airflow orchestration of Spark jobs is explicitly deferred to Plan 9, per the Plan 1 review) loads that seed into an Iceberg `dim_game` table.

**Tech Stack:** Python 3 (`requests`, stdlib `csv`), `pytest` + `responses` for testing, RAWG API, Steam's storesearch endpoint (unofficial), PySpark + Iceberg (existing Plan 1 cluster).

**Spec:** `docs/superpowers/specs/2026-11-03-gaming-price-intelligence-design.md`

## Global Constraints

- Discovery is API-driven, not hand-typed; ambiguous matches always get a human review pass before entering `dim_game` — nothing is auto-accepted on a low-confidence match. [spec: Non-goals]
- `dim_game` schema is fixed: `game_key, canonical_name, coverage, steam_appid, platprices_ppid, rawg_id, added_date`. [spec: Scope]
- **This plan covers only the `BOTH` and `STEAM_ONLY` tiers.** PlatPrices-dependent work (PS5 matching, the `PS5_ONLY` tier, populating `platprices_ppid`) is deferred until the PlatPrices API key is approved — it is pending manual review as of this writing, and PlatPrices explicitly does not issue keys for personal/local-only tools, so approval isn't guaranteed. This is a scope decision made with the project owner, not an oversight — **it must be completed later**, not dropped. Every `dim_game` row this plan produces has `platprices_ppid = NULL`.
- Steam's storesearch endpoint (`store.steampowered.com/api/storesearch`) is unofficial and undocumented, same category of risk as Steam's `appdetails` endpoint from the spec — poll defensively, don't assume its shape is stable.
- Runs fully locally. The discovery script itself needs no Docker (plain Python, run from a local virtualenv); only the final Iceberg load step touches the Plan 1 docker-compose stack.
- No hardcoded credentials — `RAWG_API_KEY` comes from `.env` (same file Plan 1 established), never committed.

**Known risks to verify live before trusting this plan's output** (RAWG was returning a Cloudflare 522 — origin down — as of this writing; confirm it's back before running Task 1's live step):
- RAWG's exact platform IDs for "PC" and "PlayStation 5" are looked up live in Task 1, not hardcoded from memory — published lists of these IDs exist but drift, and getting this wrong silently poisons every downstream classification.
- Steam's storesearch response shape (field names, pagination, rate limits) is based on community usage, not official documentation.

## Review Focus

- **RAWG pagination silently truncated**: stopping before exhausting real results (or before hitting a sane cap) would silently shrink the candidate pool with no error. Covered by: Task 1's pagination test, which asserts the client follows RAWG's `next` link until exhausted or a `max_pages` cap.
- **Name-matching false positives**: a sequel or remaster (e.g. "Dark Souls" vs "Dark Souls III") auto-accepted as the same game would corrupt `dim_game` silently. Covered by: Task 3's `best_match` tests, which include a near-miss pair that must NOT be classified high-confidence.
- **CSV round-trip corruption**: a game title containing a comma or quote breaking column alignment on write/read. Covered by: Task 2's CSV write/read test, using a fixture title that contains a comma.
- **Manual review decisions silently overwritten on re-run**: re-running discovery after a human has already corrected some ambiguous matches must not discard those corrections. Covered by: Task 4's merge test, asserting a human review decision always wins over a fresh auto-match for the same `rawg_id`.
- **Non-idempotent Iceberg load**: re-running the load job must not duplicate rows. Covered by: Task 5's live verification, which runs the load job twice against the same seed and asserts the row count is unchanged.

---

## Task 1: RAWG client — platform lookup and paginated game fetch

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `pytest.ini`
- Create: `collectors/__init__.py`
- Create: `collectors/discovery/__init__.py`
- Create: `collectors/discovery/rawg_client.py`
- Create: `tests/__init__.py`
- Create: `tests/discovery/__init__.py`
- Create: `tests/discovery/test_rawg_client.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (for Task 2):
  - `rawg_client.fetch_platform_id(name: str, api_key: str) -> int | None` — looks up a platform's numeric RAWG ID by its display name via `GET /platforms`. Returns `None` if no platform matches.
  - `rawg_client.fetch_games_by_platform(platform_id: int, api_key: str, max_pages: int = 20) -> list[dict]` — paginated fetch of `GET /games?platforms=<id>`, following RAWG's `next` URL until exhausted or `max_pages` reached. Each returned dict has keys `id` (int), `name` (str), `platforms` (`list[int]` — flattened from RAWG's nested `{platform: {id, name}}` objects).

- [ ] **Step 1: Set up the Python environment**

Run:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Write `requirements.txt`:
```
requests==2.32.3
```

Write `requirements-dev.txt`:
```
-r requirements.txt
pytest==8.3.3
responses==0.25.3
```

Write `pytest.ini`:
```ini
[pytest]
testpaths = tests
```

Run: `pip install -r requirements-dev.txt`
Expected: installs cleanly, no errors.

Create empty `collectors/__init__.py`, `collectors/discovery/__init__.py`, `tests/__init__.py`, `tests/discovery/__init__.py`.

- [ ] **Step 2: Write the failing test for `fetch_platform_id`**

```python
# tests/discovery/test_rawg_client.py
import responses

from collectors.discovery.rawg_client import fetch_platform_id


@responses.activate
def test_fetch_platform_id_matches_by_name():
    responses.add(
        responses.GET,
        "https://api.rawg.io/api/platforms",
        json={"results": [{"id": 4, "name": "PC"}, {"id": 187, "name": "PlayStation 5"}]},
        status=200,
    )

    assert fetch_platform_id("PlayStation 5", api_key="test-key") == 187


@responses.activate
def test_fetch_platform_id_returns_none_when_not_found():
    responses.add(
        responses.GET,
        "https://api.rawg.io/api/platforms",
        json={"results": [{"id": 4, "name": "PC"}]},
        status=200,
    )

    assert fetch_platform_id("Nonexistent Platform", api_key="test-key") is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/discovery/test_rawg_client.py -v`
Expected: both FAIL with `ModuleNotFoundError` or `ImportError` (`fetch_platform_id` not defined).

- [ ] **Step 4: Implement `fetch_platform_id` in `collectors/discovery/rawg_client.py`**

```python
import requests

RAWG_BASE_URL = "https://api.rawg.io/api"


def fetch_platform_id(name: str, api_key: str) -> int | None:
    ...
```

Call `GET {RAWG_BASE_URL}/platforms` with `params={"key": api_key}`, iterate `response.json()["results"]`, return the `id` of the entry whose `name` matches (case-insensitive exact match), or `None`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/discovery/test_rawg_client.py -v`
Expected: both PASS.

- [ ] **Step 6: Write the failing tests for `fetch_games_by_platform`**

First, update the top-of-file import to add the new name alongside the existing one:
```python
from collectors.discovery.rawg_client import fetch_platform_id, fetch_games_by_platform
```
(Importing a name that doesn't exist yet would break collection for the whole file — including the already-passing `fetch_platform_id` tests — so this import update happens in the same step as adding the new tests, before running anything.)

```python
@responses.activate
def test_fetch_games_by_platform_flattens_platform_ids():
    responses.add(
        responses.GET,
        "https://api.rawg.io/api/games",
        json={
            "results": [
                {"id": 1, "name": "Elden Ring", "platforms": [{"platform": {"id": 4, "name": "PC"}}, {"platform": {"id": 187, "name": "PlayStation 5"}}]},
            ],
            "next": None,
        },
        status=200,
    )

    games = fetch_games_by_platform(platform_id=4, api_key="test-key")

    assert games == [{"id": 1, "name": "Elden Ring", "platforms": [4, 187]}]


@responses.activate
def test_fetch_games_by_platform_follows_pagination():
    responses.add(
        responses.GET,
        "https://api.rawg.io/api/games",
        json={"results": [{"id": 1, "name": "Game One", "platforms": [{"platform": {"id": 4, "name": "PC"}}]}], "next": "https://api.rawg.io/api/games?platforms=4&page=2"},
        status=200,
    )
    responses.add(
        responses.GET,
        "https://api.rawg.io/api/games?platforms=4&page=2",
        json={"results": [{"id": 2, "name": "Game Two", "platforms": [{"platform": {"id": 4, "name": "PC"}}]}], "next": None},
        status=200,
    )

    games = fetch_games_by_platform(platform_id=4, api_key="test-key")

    assert [g["id"] for g in games] == [1, 2]


@responses.activate
def test_fetch_games_by_platform_stops_at_max_pages():
    responses.add(
        responses.GET,
        "https://api.rawg.io/api/games",
        json={"results": [{"id": 1, "name": "Game One", "platforms": []}], "next": "https://api.rawg.io/api/games?platforms=4&page=2"},
        status=200,
    )

    games = fetch_games_by_platform(platform_id=4, api_key="test-key", max_pages=1)

    assert len(responses.calls) == 1
    assert [g["id"] for g in games] == [1]
```

- [ ] **Step 7: Run tests to verify they fail**

Run: `pytest tests/discovery/test_rawg_client.py -v`
Expected: the three new tests FAIL (`fetch_games_by_platform` not defined).

- [ ] **Step 8: Implement `fetch_games_by_platform`**

```python
def fetch_games_by_platform(platform_id: int, api_key: str, max_pages: int = 20) -> list[dict]:
    ...
```

Fetch `GET {RAWG_BASE_URL}/games` with `params={"key": api_key, "platforms": platform_id, "page_size": 40}` for the first page; for each page, flatten each result's `platforms` field from `[{"platform": {"id": ..., "name": ...}}, ...]` to a plain `list[int]` of IDs, and collect `{"id", "name", "platforms"}` dicts. Follow the response's `next` URL (pass it directly as the next request's URL, no params needed since it's a full URL) until `next` is `None` or `max_pages` pages have been fetched.

- [ ] **Step 9: Run tests to verify they pass**

Run: `pytest tests/discovery/test_rawg_client.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 10: Live verification — confirm RAWG is back up and get real platform IDs**

This step needs a real `RAWG_API_KEY` (added to `.env`, following the same pattern as Plan 1's `.env`/`.env.example`) and RAWG's service to be responding (it was returning a Cloudflare 522 as of this writing).

Run:
```bash
python3 -c "
import os
from collectors.discovery.rawg_client import fetch_platform_id
key = os.environ['RAWG_API_KEY']
print('PC:', fetch_platform_id('PC', key))
print('PlayStation 5:', fetch_platform_id('PlayStation 5', key))
"
```
Expected: two integer IDs printed, no errors. Record both — Task 2 needs them. If RAWG still returns a 522, wait and retry; this step cannot be skipped since Task 2 depends on knowing the real IDs, not assumed ones.

- [ ] **Step 11: Commit**

```bash
git add requirements.txt requirements-dev.txt pytest.ini collectors/ tests/ .gitignore
git commit -m "discovery: add RAWG client with platform lookup and paginated game fetch"
```

(Add `.venv/` to `.gitignore` if not already covered.)

---

## Task 2: Candidate classification and CSV output

**Files:**
- Modify: `collectors/discovery/build_seed.py` (create)
- Create: `tests/discovery/test_build_seed.py`

**Interfaces:**
- Consumes: Task 1's `rawg_client.fetch_games_by_platform` (patched/mocked in tests).
- Produces (for Task 3):
  - `build_seed.classify_coverage(platforms: list[int], pc_id: int, ps5_id: int) -> str` — returns `"BOTH"` if both `pc_id` and `ps5_id` are in `platforms`, `"STEAM_ONLY"` if only `pc_id` is, `"OTHER"` otherwise (neither, or PS5-only — PS5-only is out of scope for this plan).
  - `build_seed.build_candidates(api_key: str, pc_id: int, ps5_id: int) -> list[dict]` — fetches PC-platform games via `fetch_games_by_platform`, classifies each, and returns `{"rawg_id": int, "canonical_name": str, "coverage": str}` for every game classified `BOTH` or `STEAM_ONLY` (discards `OTHER`).
  - `build_seed.write_candidates_csv(candidates: list[dict], path: str) -> None` — writes `rawg_id,canonical_name,coverage` with a header row, using the stdlib `csv` module (handles quoting for titles containing commas).

- [ ] **Step 1: Write the failing tests for `classify_coverage`**

```python
# tests/discovery/test_build_seed.py
from collectors.discovery.build_seed import classify_coverage


def test_classify_coverage_both():
    assert classify_coverage([4, 187], pc_id=4, ps5_id=187) == "BOTH"


def test_classify_coverage_steam_only():
    assert classify_coverage([4, 1], pc_id=4, ps5_id=187) == "STEAM_ONLY"


def test_classify_coverage_other():
    assert classify_coverage([187], pc_id=4, ps5_id=187) == "OTHER"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `classify_coverage`**

Pure function, implements the logic described in Interfaces above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: the 3 `classify_coverage` tests PASS.

- [ ] **Step 5: Write the failing test for `build_candidates`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import classify_coverage, build_candidates
```

```python
from unittest.mock import patch


def test_build_candidates_discards_other_and_classifies():
    fake_games = [
        {"id": 1, "name": "Elden Ring", "platforms": [4, 187]},
        {"id": 2, "name": "Helldivers 2", "platforms": [4, 187]},
        {"id": 3, "name": "A PC Exclusive", "platforms": [4]},
        {"id": 4, "name": "Not On PC", "platforms": [187]},
    ]
    with patch("collectors.discovery.build_seed.fetch_games_by_platform", return_value=fake_games) as mock_fetch:
        result = build_candidates(api_key="test-key", pc_id=4, ps5_id=187)

    mock_fetch.assert_called_once_with(platform_id=4, api_key="test-key")
    assert result == [
        {"rawg_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH"},
        {"rawg_id": 2, "canonical_name": "Helldivers 2", "coverage": "BOTH"},
        {"rawg_id": 3, "canonical_name": "A PC Exclusive", "coverage": "STEAM_ONLY"},
    ]
```

- [ ] **Step 6: Run test to verify it fails**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: FAILS (`build_candidates` not defined).

- [ ] **Step 7: Implement `build_candidates`**

Import `fetch_games_by_platform` from `collectors.discovery.rawg_client` at module level (so the `patch` target in the test resolves correctly). Call it once with `platform_id=pc_id`, classify each result with `classify_coverage`, filter out `"OTHER"`.

- [ ] **Step 8: Run test to verify it passes**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: PASSES.

- [ ] **Step 9: Write the failing test for `write_candidates_csv`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import classify_coverage, build_candidates, write_candidates_csv
```

```python
import csv


def test_write_candidates_csv_round_trips_commas_in_titles(tmp_path):
    candidates = [
        {"rawg_id": 1, "canonical_name": "Tom Clancy's, The Division", "coverage": "BOTH"},
    ]
    path = tmp_path / "candidates.csv"

    write_candidates_csv(candidates, str(path))

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [{"rawg_id": "1", "canonical_name": "Tom Clancy's, The Division", "coverage": "BOTH"}]
```

- [ ] **Step 10: Run test to verify it fails**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: FAILS (`write_candidates_csv` not defined).

- [ ] **Step 11: Implement `write_candidates_csv`**

Use `csv.DictWriter` with `fieldnames=["rawg_id", "canonical_name", "coverage"]`, `writeheader()`, then `writerows(candidates)`.

- [ ] **Step 12: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all PASS.

- [ ] **Step 13: Commit**

```bash
git add collectors/discovery/build_seed.py tests/discovery/test_build_seed.py
git commit -m "discovery: classify RAWG candidates by platform coverage and write to CSV"
```

---

## Task 3: Steam verification and match confidence

**Files:**
- Create: `collectors/discovery/steam_match.py`
- Create: `tests/discovery/test_steam_match.py`
- Modify: `collectors/discovery/build_seed.py` (add `match_candidates`)
- Modify: `tests/discovery/test_build_seed.py`

**Interfaces:**
- Consumes: Task 2's candidate dicts (`rawg_id, canonical_name, coverage`).
- Produces (for Task 4):
  - `steam_match.normalize_name(name: str) -> str` — lowercase; strips a fixed list of edition/suffix markers (`": deluxe edition"`, `": complete edition"`, `": remastered"`, `": director's cut"`, `": game of the year edition"` — case-insensitive) and surrounding punctuation/whitespace.
  - `steam_match.search_steam(name: str) -> list[dict]` — `GET https://store.steampowered.com/api/storesearch/?term=<name>&cc=us&l=en`, returns `response.json()["items"]` as `[{"id": int, "name": str}, ...]` (Steam's field is `id`, not `appid`, in this endpoint).
  - `steam_match.best_match(rawg_name: str, steam_results: list[dict]) -> tuple[dict | None, bool]` — returns `(None, False)` if `steam_results` is empty; otherwise returns the first result and `True` if its normalized name equals the normalized `rawg_name`, else the first result and `False`.
  - `build_seed.match_candidates(candidates: list[dict]) -> tuple[list[dict], list[dict]]` — for each candidate, calls `search_steam` then `best_match`; returns `(matched, needs_review)` where `matched` entries gain a `steam_appid` key (high-confidence only) and `needs_review` entries gain a `steam_candidates` key (the raw list of Steam results, for a human to pick from).

- [ ] **Step 1: Write the failing tests for `normalize_name`**

```python
# tests/discovery/test_steam_match.py
from collectors.discovery.steam_match import normalize_name


def test_normalize_name_strips_edition_suffix():
    assert normalize_name("Elden Ring: Deluxe Edition") == "elden ring"


def test_normalize_name_lowercases_and_trims():
    assert normalize_name("  Hollow Knight  ") == "hollow knight"


def test_normalize_name_leaves_sequel_numbers_alone():
    assert normalize_name("Dark Souls III") == "dark souls iii"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: all FAIL.

- [ ] **Step 3: Implement `normalize_name`**

```python
EDITION_SUFFIXES = [
    ": deluxe edition", ": complete edition", ": remastered",
    ": director's cut", ": game of the year edition",
]


def normalize_name(name: str) -> str:
    ...
```

Lowercase the input, strip each suffix in `EDITION_SUFFIXES` if present at the end, strip surrounding whitespace.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: the 3 `normalize_name` tests PASS.

- [ ] **Step 5: Write the failing tests for `search_steam`**

First, update the top-of-file import and add `responses`:
```python
import responses

from collectors.discovery.steam_match import normalize_name, search_steam
```

```python
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
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: FAIL (`search_steam` not defined).

- [ ] **Step 7: Implement `search_steam`**

`GET` the endpoint with `params={"term": name, "cc": "us", "l": "en"}`, return `response.json()["items"]`.

- [ ] **Step 8: Run tests to verify they pass**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: PASS.

- [ ] **Step 9: Write the failing tests for `best_match`**

First, update the top-of-file import:
```python
from collectors.discovery.steam_match import normalize_name, search_steam, best_match
```

```python
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
```

- [ ] **Step 10: Run tests to verify they fail**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: FAIL (`best_match` not defined).

- [ ] **Step 11: Implement `best_match`**

Return `(None, False)` on empty input. Otherwise compare `normalize_name(rawg_name)` to `normalize_name(results[0]["name"])`; return `(results[0], True)` if equal, else `(results[0], False)`.

- [ ] **Step 12: Run tests to verify they pass**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: all PASS.

- [ ] **Step 13: Write the failing test for `build_seed.match_candidates`**

First, update the top-of-file import in `tests/discovery/test_build_seed.py` to add the new name alongside the three already there:
```python
from collectors.discovery.build_seed import classify_coverage, build_candidates, write_candidates_csv, match_candidates
```

```python
# appended to tests/discovery/test_build_seed.py
from unittest.mock import patch


def test_match_candidates_splits_by_confidence():
    candidates = [
        {"rawg_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH"},
        {"rawg_id": 2, "canonical_name": "Dark Souls III", "coverage": "BOTH"},
    ]

    def fake_search(name):
        if name == "Elden Ring":
            return [{"id": 1245620, "name": "Elden Ring"}]
        return [{"id": 999, "name": "Dark Souls"}]

    with patch("collectors.discovery.build_seed.search_steam", side_effect=fake_search):
        matched, needs_review = match_candidates(candidates)

    assert matched == [
        {"rawg_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620},
    ]
    assert needs_review == [
        {
            "rawg_id": 2,
            "canonical_name": "Dark Souls III",
            "coverage": "BOTH",
            "steam_candidates": [{"id": 999, "name": "Dark Souls"}],
        },
    ]
```

- [ ] **Step 14: Run test to verify it fails**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: FAILS (`match_candidates` not defined).

- [ ] **Step 15: Implement `match_candidates`**

Import `search_steam` and `best_match` from `collectors.discovery.steam_match` at module level in `build_seed.py`. For each candidate, call `search_steam(candidate["canonical_name"])` then `best_match(candidate["canonical_name"], results)`; append to `matched` with a `steam_appid` key if confident, else append to `needs_review` with a `steam_candidates` key holding the raw results.

- [ ] **Step 16: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py tests/discovery/test_steam_match.py -v`
Expected: all PASS.

- [ ] **Step 17: Commit**

```bash
git add collectors/discovery/steam_match.py tests/discovery/test_steam_match.py collectors/discovery/build_seed.py tests/discovery/test_build_seed.py
git commit -m "discovery: verify RAWG candidates against Steam storesearch with confidence scoring"
```

---

## Task 4: Manual review merge and seed file finalization

**Files:**
- Create: `collectors/discovery/finalize_seed.py`
- Create: `tests/discovery/test_finalize_seed.py`

**Interfaces:**
- Consumes: Task 3's `matched`/`needs_review` lists.
- Produces (for Task 5):
  - `finalize_seed.merge_reviewed(matched: list[dict], needs_review: list[dict], review_decisions: dict[int, int | None]) -> list[dict]` — `review_decisions` maps `rawg_id → corrected steam_appid` (human's pick from `steam_candidates`) or `None` (human rejected — game excluded). Returns the combined list of `{"rawg_id", "canonical_name", "coverage", "steam_appid"}` dicts: every `matched` entry, plus every `needs_review` entry whose `rawg_id` has a non-`None` decision. A `rawg_id` with no entry in `review_decisions` at all is treated as still pending and excluded (not silently guessed at).
  - `finalize_seed.to_dim_game_rows(merged: list[dict], added_date: str) -> list[dict]` — maps each merged dict to the final schema: `game_key` (lowercase `canonical_name` with non-alphanumeric runs collapsed to single underscores, e.g. `"Elden Ring"` → `"elden_ring"`), `canonical_name`, `coverage`, `steam_appid`, `platprices_ppid` (always `None`, per this plan's scope), `rawg_id`, `added_date`.
  - `finalize_seed.write_seed_csv(rows: list[dict], path: str) -> None` — writes with header `game_key,canonical_name,coverage,steam_appid,platprices_ppid,rawg_id,added_date`, empty string for `platprices_ppid`.

- [ ] **Step 1: Write the failing tests for `merge_reviewed`**

```python
# tests/discovery/test_finalize_seed.py
from collectors.discovery.finalize_seed import merge_reviewed


def test_merge_reviewed_includes_matched_and_accepted_reviews():
    matched = [{"rawg_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620}]
    needs_review = [
        {"rawg_id": 2, "canonical_name": "Dark Souls III", "coverage": "BOTH", "steam_candidates": [{"id": 374320, "name": "DARK SOULS III"}]},
        {"rawg_id": 3, "canonical_name": "Fake Game", "coverage": "STEAM_ONLY", "steam_candidates": []},
    ]
    review_decisions = {2: 374320, 3: None}

    result = merge_reviewed(matched, needs_review, review_decisions)

    assert result == [
        {"rawg_id": 1, "canonical_name": "Elden Ring", "coverage": "BOTH", "steam_appid": 1245620},
        {"rawg_id": 2, "canonical_name": "Dark Souls III", "coverage": "BOTH", "steam_appid": 374320},
    ]


def test_merge_reviewed_excludes_undecided_reviews():
    matched = []
    needs_review = [{"rawg_id": 5, "canonical_name": "Pending Game", "coverage": "BOTH", "steam_candidates": []}]

    result = merge_reviewed(matched, needs_review, review_decisions={})

    assert result == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: both FAIL.

- [ ] **Step 3: Implement `merge_reviewed`**

Start with a copy of `matched`. For each entry in `needs_review`, look up `review_decisions.get(entry["rawg_id"])`; if present and not `None`, append `{"rawg_id", "canonical_name", "coverage", "steam_appid": <decision>}` (dropping `steam_candidates`). If absent or `None`, skip.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: both PASS.

- [ ] **Step 5: Write the failing test for `to_dim_game_rows`**

First, update the top-of-file import:
```python
from collectors.discovery.finalize_seed import merge_reviewed, to_dim_game_rows
```

```python
def test_to_dim_game_rows_builds_game_key_and_null_platprices():
    merged = [{"rawg_id": 1, "canonical_name": "Elden Ring: Deluxe Edition", "coverage": "BOTH", "steam_appid": 1245620}]

    rows = to_dim_game_rows(merged, added_date="2026-11-04")

    assert rows == [{
        "game_key": "elden_ring_deluxe_edition",
        "canonical_name": "Elden Ring: Deluxe Edition",
        "coverage": "BOTH",
        "steam_appid": 1245620,
        "platprices_ppid": None,
        "rawg_id": 1,
        "added_date": "2026-11-04",
    }]
```

- [ ] **Step 6: Run test to verify it fails**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: FAILS (`to_dim_game_rows` not defined).

- [ ] **Step 7: Implement `to_dim_game_rows`**

Build `game_key` with `re.sub(r"[^a-z0-9]+", "_", canonical_name.lower()).strip("_")`. Set `platprices_ppid` to `None` unconditionally (this plan's scope).

- [ ] **Step 8: Run test to verify it passes**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: PASSES.

- [ ] **Step 9: Write the failing test for `write_seed_csv`**

First, update the top-of-file import:
```python
from collectors.discovery.finalize_seed import merge_reviewed, to_dim_game_rows, write_seed_csv
```

```python
def test_write_seed_csv_writes_expected_header_and_empty_platprices(tmp_path):
    rows = [{
        "game_key": "elden_ring", "canonical_name": "Elden Ring", "coverage": "BOTH",
        "steam_appid": 1245620, "platprices_ppid": None, "rawg_id": 1, "added_date": "2026-11-04",
    }]
    path = tmp_path / "dim_game_seed.csv"

    write_seed_csv(rows, str(path))

    content = path.read_text()
    assert content.splitlines()[0] == "game_key,canonical_name,coverage,steam_appid,platprices_ppid,rawg_id,added_date"
    assert "elden_ring,Elden Ring,BOTH,1245620,,1,2026-11-04" in content
```

- [ ] **Step 10: Run test to verify it fails**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: FAILS (`write_seed_csv` not defined).

- [ ] **Step 11: Implement `write_seed_csv`**

`csv.DictWriter` with the fixed fieldnames from Interfaces; write `""` where the row's value is `None` (`csv.DictWriter` does this automatically for `None` values, but confirm it against the test rather than assuming).

- [ ] **Step 12: Run tests to verify they pass**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: all PASS.

- [ ] **Step 13: Commit**

```bash
git add collectors/discovery/finalize_seed.py tests/discovery/test_finalize_seed.py
git commit -m "discovery: merge human review decisions and finalize the dim_game seed CSV"
```

---

## Task 5: Load the seed into an Iceberg `dim_game` table

**Files:**
- Create: `infra/spark/jobs/load_dim_game.py`
- Create: `infra/spark/jobs/fixtures/dim_game_seed_fixture.csv`
- Modify: `docker-compose.yml` (mount `./infra/spark/jobs` and a `data/` directory into `spark-master`)

**Interfaces:**
- Consumes: a seed CSV matching Task 4's schema, at a path passed via `--seed-path` argument.
- Produces: an Iceberg table `lakehouse.dim.dim_game`, fully replaced (not appended) on every run — the same truncate-and-reload reasoning as the spec's ClickHouse sync, since `dim_game` is small and meant to be reloaded whenever the seed file changes, not accumulated into.

- [ ] **Step 1: Add a fixture seed CSV for infra verification, independent of real discovery data**

`infra/spark/jobs/fixtures/dim_game_seed_fixture.csv`:
```csv
game_key,canonical_name,coverage,steam_appid,platprices_ppid,rawg_id,added_date
elden_ring,Elden Ring,BOTH,1245620,,1,2026-11-04
hollow_knight,Hollow Knight,STEAM_ONLY,367520,,2,2026-11-04
```

This fixture proves the loading mechanism works independently of whether Tasks 1-4's live discovery run has happened yet (it hasn't, pending RAWG/the API key) — same reasoning as Plan 1's `smoke_test.py` using synthetic data rather than real business data.

- [ ] **Step 2: Write `infra/spark/jobs/load_dim_game.py`**

```python
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

SCHEMA = StructType([
    StructField("game_key", StringType(), False),
    StructField("canonical_name", StringType(), False),
    StructField("coverage", StringType(), False),
    StructField("steam_appid", IntegerType(), True),
    StructField("platprices_ppid", StringType(), True),
    StructField("rawg_id", IntegerType(), False),
    StructField("added_date", StringType(), False),
])


def main(seed_path: str) -> None:
    spark = SparkSession.builder.appName("load-dim-game").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dim")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.dim.dim_game (
            game_key STRING, canonical_name STRING, coverage STRING,
            steam_appid INT, platprices_ppid STRING, rawg_id INT, added_date DATE
        ) USING iceberg
    """)

    df = spark.read.csv(seed_path, header=True, schema=SCHEMA)
    df = df.withColumn("added_date", df["added_date"].cast("date"))
    df.createOrReplaceTempView("seed_source")
    spark.sql("INSERT OVERWRITE lakehouse.dim.dim_game SELECT * FROM seed_source")

    count = spark.sql("SELECT COUNT(*) AS n FROM lakehouse.dim.dim_game").collect()[0]["n"]
    print(f"LOAD DIM_GAME PASSED: {count} rows")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
```

- [ ] **Step 3: Mount the jobs and fixtures directory into `spark-master`**

`infra/spark/smoke_test.py` and `shuffle_test.py` are already reachable at `/opt/spark-apps` via the existing `./infra/spark:/opt/spark-apps` mount in `docker-compose.yml` (Plan 1) — `infra/spark/jobs/` is a subdirectory of that same mount, so no new volume entry is needed. Confirm this rather than assuming:

Run: `docker compose exec spark-master ls /opt/spark-apps/jobs`
Expected: lists `load_dim_game.py` and `fixtures/dim_game_seed_fixture.csv`.

- [ ] **Step 4: Run the load job against the fixture and verify row count**

Run: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/jobs/load_dim_game.py /opt/spark-apps/jobs/fixtures/dim_game_seed_fixture.csv`
Expected: stdout includes `LOAD DIM_GAME PASSED: 2 rows`.

- [ ] **Step 5: Run the load job a second time and verify idempotency (Review Focus item)**

Run the same command from Step 4 again.
Expected: stdout again includes `LOAD DIM_GAME PASSED: 2 rows` — not 4. `INSERT OVERWRITE` replaces the table's contents rather than appending, so re-running with the same fixture must not double the row count.

- [ ] **Step 6: Commit**

```bash
git add infra/spark/jobs/
git commit -m "discovery: add Iceberg dim_game load job, verified idempotent against a fixture seed"
```

---

## What's left to the builder / explicitly deferred

- **PlatPrices-dependent work**: PS5 candidate discovery (RAWG PS5-platform-only query), PlatPrices PPID matching, the `PS5_ONLY` tier, and populating `platprices_ppid` for `BOTH`-tier games — blocked on PlatPrices API key approval. Do not forget this; it is a deferred increment, not dropped scope.
- **The actual live discovery run** (Tasks 1-4 run for real, against real RAWG data, producing the real `data/dim_game_seed.csv`) is a manual step the project owner runs once RAWG is confirmed back up — this plan delivers the tooling, not a populated seed file.
- **Airflow orchestration of the load job** is deferred to Plan 9 per the Plan 1 review's explicit ruling (Airflow has no Spark/Java in its image; solving that generically belongs with the batch-chain DAG design, not bolted on here for one job).
- The exact number of `BOTH`/`STEAM_ONLY` games this produces is whatever RAWG's real data yields — the spec's `M`/`y` tier-sizing formula depends on knowing the verified mutual count, which isn't final until the PlatPrices-dependent work above completes.
