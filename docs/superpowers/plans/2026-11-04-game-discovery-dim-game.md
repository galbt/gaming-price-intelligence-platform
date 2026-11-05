# Game Discovery → dim_game (IGDB + Steam) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an API-driven game discovery pipeline that produces a hand-reviewed `dim_game` seed file covering **all three coverage tiers** (`BOTH`, `STEAM_ONLY`, `PS5_ONLY`), and load it into an Iceberg `dim_game` table on the Plan 1 infrastructure.

**Revision note (2026-11-05):** This plan originally targeted RAWG + PlatPrices and deferred the `PS5_ONLY` tier because PlatPrices required a paid key. Both assumptions changed during design validation: PlatPrices was confirmed paid-only (no free tier at all) and RAWG suffered an extended outage. The design now uses **IGDB** for discovery/metadata (free, confirmed live) and a direct **PlayStation Store page fetch** for PS5 pricing (not built in this plan — see Task 2/3 below for what IGDB gives us at discovery time). Because IGDB's `websites` field supplies a direct PS Store URL per game, the `PS5_ONLY` tier no longer needs to be deferred — it's in scope here.

**Architecture:** A local Python script queries IGDB's `/v4/games` endpoint filtered by platform (once for PC, once for PS5), extracts each result's direct Steam/PlayStation Store URLs from its `websites` field, classifies three-tier coverage (treating a resolvable PS Store URL as proof of real PS5 availability even when IGDB's own `platforms` field omits it — a confirmed gap, see Global Constraints), detects games that collide on the same `game_key` (two distinct IGDB entries sharing an identical title — a confirmed real case), falls back to Steam's `storesearch` only for the subset of candidates where IGDB's own data didn't supply a Steam URL, routes everything ambiguous to a human-reviewed CSV, then a finalize step merges review decisions into a committed seed file. A PySpark batch job (run manually via `spark-submit` against the Plan 1 cluster — Airflow orchestration of Spark jobs is explicitly deferred to Plan 9, per the Plan 1 review) loads that seed into an Iceberg `dim_game` table.

**Tech Stack:** Python 3 (`requests`, stdlib `csv`, `re`), `pytest` + `responses` for testing, IGDB API (via Twitch OAuth2 client-credentials), Steam's storesearch endpoint (unofficial, fallback only), PySpark + Iceberg (existing Plan 1 cluster).

**Spec:** `docs/superpowers/specs/2026-11-03-gaming-price-intelligence-design.md`

## Global Constraints

- Discovery is API-driven, not hand-typed; ambiguous matches always get a human review pass before entering `dim_game` — nothing is auto-accepted on a low-confidence match. [spec: Non-goals]
- `dim_game` schema is fixed: `game_key, canonical_name, coverage, steam_appid, ps_store_path, igdb_id, added_date`. [spec: Scope] `ps_store_path` is the trailing URL segment after the locale (e.g. `"concept/10000333"` or `"product/EP1004-...-STANDARD001"`) — later plans build the fetch URL as `store.playstation.com/<locale>/<ps_store_path>`.
- **IGDB's `platforms` field is not a reliable sole signal for PS5 availability.** Confirmed during design: *Red Dead Redemption 2* has no `PlayStation 5` entry in its `platforms` list despite having a live, resolvable PS Store URL (it's sold via PS4 backward compatibility). Coverage classification must treat "a PS Store URL resolves from `websites`" as an independent, overriding signal — not rely on platform-query membership alone. [spec: Anti-goals]
- **IGDB can list two unrelated games under an identical title.** Confirmed during design: the 2005 original *Resident Evil 4* and the 2023 remake both appear as "Resident Evil 4". Since `game_key` is a slug of `canonical_name`, two such entries would collide on the same `game_key` if both were auto-accepted — this must be detected and routed to human review, not silently loaded. [spec: Anti-goals]
- **IGDB's `websites` field is not guaranteed complete.** Confirmed during design: *Marvel's Spider-Man Remastered* had a PS Store URL but no Steam URL in IGDB's data despite definitely having a Steam listing. Steam's `storesearch` is a **fallback**, used only when IGDB's own data doesn't supply a Steam URL for a game already confirmed on PC — not the primary matching mechanism.
- There is no automated fallback for a missing PS Store URL (the PlayStation Store's own search is client-side rendered and was ruled out as too fragile during design — see spec Risks). A `PS5_ONLY` or `BOTH` candidate with no resolvable PS Store URL always goes to manual review for a human to supply the URL, or reject the candidate.
- Steam's storesearch endpoint (`store.steampowered.com/api/storesearch`) is unofficial and undocumented, same category of risk as Steam's `appdetails` endpoint from the spec — poll defensively, don't assume its shape is stable.
- Runs fully locally. The discovery script itself needs no Docker (plain Python, run from a local virtualenv); only the final Iceberg load step touches the Plan 1 docker-compose stack.
- No hardcoded credentials — `IGDB_CLIENT_ID`/`IGDB_CLIENT_SECRET` come from `.env` (same file Plan 1 established; already populated for this project), never committed.

**Known risks to verify live before trusting this plan's output:**
- IGDB's exact platform IDs for "PC" and "PlayStation 5" are looked up live in Task 1, not hardcoded from memory.
- IGDB's `/v4/games` response omits fields at their zero/default value (confirmed during design: `category` never appeared in any response, even for genuine DLC/edition entries) — do not rely on `category` for edition filtering; this plan uses `websites`/`platforms` presence instead, which were confirmed populated.
- Steam's storesearch response shape (field names, pagination, rate limits) is based on community usage, not official documentation.

## Review Focus

- **`game_key` collisions silently loading two unrelated games as one row**: a sequel/remake sharing an identical title (confirmed real case: "Resident Evil 4," 2005 vs. 2023) auto-accepted without a collision check would corrupt `dim_game` with an ambiguous or duplicate key. Covered by: Task 2's `find_game_key_collisions` tests.
- **PS5 backward-compatible availability undercounted**: a game sold on PS5 only via backward compatibility (confirmed real case: *Red Dead Redemption 2*, no `PlayStation 5` in `platforms`) misclassified as not-on-PS5 because classification trusted the platform tag alone. Covered by: Task 2's `classify_coverage`/`build_candidates` tests, which include a platforms-absent-but-URL-present case.
- **Name-matching false positives in the Steam fallback**: a sequel or remaster (e.g. "Dark Souls" vs "Dark Souls III") auto-accepted as the same game would corrupt `dim_game` silently. Covered by: Task 3's `best_match` tests, which include a near-miss pair that must NOT be classified high-confidence.
- **CSV round-trip corruption**: a game title containing a comma or quote breaking column alignment on write/read. Covered by: Task 2's CSV write/read test, using a fixture title that contains a comma.
- **Manual review decisions silently overwritten on re-run**: re-running discovery after a human has already corrected some ambiguous matches must not discard those corrections. Covered by: Task 4's merge test, asserting a human review decision always wins over a fresh auto-match for the same `igdb_id`.

---

## Task 1: IGDB client — OAuth token, platform lookup, paginated game fetch

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `pytest.ini`
- Create: `collectors/__init__.py`
- Create: `collectors/discovery/__init__.py`
- Create: `collectors/discovery/igdb_client.py`
- Create: `tests/__init__.py`
- Create: `tests/discovery/__init__.py`
- Create: `tests/discovery/test_igdb_client.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (for Task 2):
  - `igdb_client.get_access_token(client_id: str, client_secret: str) -> str` — `POST https://id.twitch.tv/oauth2/token` with `client_id`, `client_secret`, `grant_type=client_credentials`; returns `response.json()["access_token"]`.
  - `igdb_client.fetch_platform_id(name: str, client_id: str, access_token: str) -> int | None` — `POST https://api.igdb.com/v4/platforms` with body `f'search "{name}"; fields name; limit 5;'`, headers `{"Client-ID": client_id, "Authorization": f"Bearer {access_token}"}`. Returns the `id` of the entry whose `name` matches case-insensitive exactly, else `None`.
  - `igdb_client.fetch_games_by_platform(platform_id: int, client_id: str, access_token: str, max_pages: int = 20) -> list[dict]` — paginated `POST https://api.igdb.com/v4/games` with body `f'fields name,platforms,websites.url; where platforms = ({platform_id}); limit 500; offset {offset};'`, incrementing `offset` by 500 each page, stopping when a page returns fewer than 500 results or `max_pages` is reached. Each returned dict has keys `id` (int), `name` (str), `platforms` (`list[int]`, `[]` if the field is absent from the response), `websites` (`list[str]` of URLs, flattened from IGDB's `[{"url": ...}, ...]` shape, `[]` if absent).

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

- [ ] **Step 2: Write the failing test for `get_access_token`**

```python
# tests/discovery/test_igdb_client.py
import responses

from collectors.discovery.igdb_client import get_access_token


@responses.activate
def test_get_access_token_returns_token_from_response():
    responses.add(
        responses.POST,
        "https://id.twitch.tv/oauth2/token",
        json={"access_token": "abc123", "expires_in": 5000000, "token_type": "bearer"},
        status=200,
    )

    assert get_access_token("client-id", "client-secret") == "abc123"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/discovery/test_igdb_client.py -v`
Expected: FAIL with `ModuleNotFoundError`/`ImportError` (`get_access_token` not defined).

- [ ] **Step 4: Implement `get_access_token` in `collectors/discovery/igdb_client.py`**

```python
import requests

IGDB_BASE_URL = "https://api.igdb.com/v4"


def get_access_token(client_id: str, client_secret: str) -> str:
    ...
```

`POST https://id.twitch.tv/oauth2/token` with `data={"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}`, return `response.json()["access_token"]`.

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/discovery/test_igdb_client.py -v`
Expected: PASS.

- [ ] **Step 6: Write the failing tests for `fetch_platform_id`**

First, update the top-of-file import:
```python
from collectors.discovery.igdb_client import get_access_token, fetch_platform_id
```

```python
@responses.activate
def test_fetch_platform_id_matches_by_name():
    responses.add(
        responses.POST,
        "https://api.igdb.com/v4/platforms",
        json=[{"id": 6, "name": "PC (Microsoft Windows)"}],
        status=200,
    )

    assert fetch_platform_id("PC (Microsoft Windows)", "client-id", "token") == 6


@responses.activate
def test_fetch_platform_id_returns_none_when_not_found():
    responses.add(
        responses.POST,
        "https://api.igdb.com/v4/platforms",
        json=[],
        status=200,
    )

    assert fetch_platform_id("Nonexistent Platform", "client-id", "token") is None
```

- [ ] **Step 7: Run tests to verify they fail**

Run: `pytest tests/discovery/test_igdb_client.py -v`
Expected: both new tests FAIL (`fetch_platform_id` not defined).

- [ ] **Step 8: Implement `fetch_platform_id`**

```python
def fetch_platform_id(name: str, client_id: str, access_token: str) -> int | None:
    ...
```

`POST {IGDB_BASE_URL}/platforms` with headers `{"Client-ID": client_id, "Authorization": f"Bearer {access_token}"}` and body `f'search "{name}"; fields name; limit 5;'`. Iterate `response.json()`, return the `id` of the entry whose `name` matches `name` case-insensitively, else `None`.

- [ ] **Step 9: Run tests to verify they pass**

Run: `pytest tests/discovery/test_igdb_client.py -v`
Expected: all PASS.

- [ ] **Step 10: Write the failing tests for `fetch_games_by_platform`**

First, update the top-of-file import:
```python
from collectors.discovery.igdb_client import get_access_token, fetch_platform_id, fetch_games_by_platform
```

```python
def test_fetch_games_by_platform_flattens_websites_and_defaults_platforms():
    responses.add(
        responses.POST,
        "https://api.igdb.com/v4/games",
        json=[
            {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": [{"url": "https://store.steampowered.com/app/1245620"}]},
            {"id": 2, "name": "No Platforms Tagged"},
        ],
        status=200,
    )

    games = fetch_games_by_platform(platform_id=6, client_id="client-id", access_token="token")

    assert games == [
        {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": ["https://store.steampowered.com/app/1245620"]},
        {"id": 2, "name": "No Platforms Tagged", "platforms": [], "websites": []},
    ]


@responses.activate
def test_fetch_games_by_platform_follows_pagination():
    page_one = [{"id": i, "name": f"Game {i}", "platforms": [6], "websites": []} for i in range(500)]
    page_two = [{"id": 500, "name": "Game 500", "platforms": [6], "websites": []}]
    responses.add(responses.POST, "https://api.igdb.com/v4/games", json=page_one, status=200)
    responses.add(responses.POST, "https://api.igdb.com/v4/games", json=page_two, status=200)

    games = fetch_games_by_platform(platform_id=6, client_id="client-id", access_token="token")

    assert len(games) == 501
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_games_by_platform_stops_at_max_pages():
    full_page = [{"id": i, "name": f"Game {i}", "platforms": [6], "websites": []} for i in range(500)]
    responses.add(responses.POST, "https://api.igdb.com/v4/games", json=full_page, status=200)

    games = fetch_games_by_platform(platform_id=6, client_id="client-id", access_token="token", max_pages=1)

    assert len(responses.calls) == 1
    assert len(games) == 500
```

- [ ] **Step 11: Run tests to verify they fail**

Run: `pytest tests/discovery/test_igdb_client.py -v`
Expected: the three new tests FAIL (`fetch_games_by_platform` not defined).

- [ ] **Step 12: Implement `fetch_games_by_platform`**

```python
def fetch_games_by_platform(platform_id: int, client_id: str, access_token: str, max_pages: int = 20) -> list[dict]:
    ...
```

Loop with `offset` starting at 0, incrementing by 500 (IGDB's max `limit`) each iteration, up to `max_pages` iterations. Each request: `POST {IGDB_BASE_URL}/games` with headers as in `fetch_platform_id`, body `f'fields name,platforms,websites.url; where platforms = ({platform_id}); limit 500; offset {offset};'`. For each result, default `platforms` to `[]` and `websites` to `[]` when the key is absent (IGDB omits default/empty fields), flatten `websites` from `[{"url": ...}, ...]` to `[str, ...]`. Stop when a page returns fewer than 500 results, or after `max_pages` pages.

- [ ] **Step 13: Run tests to verify they pass**

Run: `pytest tests/discovery/test_igdb_client.py -v`
Expected: all 6 tests PASS.

- [ ] **Step 14: Live verification — confirm IGDB credentials work and get real platform IDs**

This step needs real `IGDB_CLIENT_ID`/`IGDB_CLIENT_SECRET` (already added to `.env`).

Run:
```bash
python3 -c "
import os
from collectors.discovery.igdb_client import get_access_token, fetch_platform_id
token = get_access_token(os.environ['IGDB_CLIENT_ID'], os.environ['IGDB_CLIENT_SECRET'])
cid = os.environ['IGDB_CLIENT_ID']
print('PC:', fetch_platform_id('PC (Microsoft Windows)', cid, token))
print('PS5:', fetch_platform_id('PlayStation 5', cid, token))
"
```
Expected: two integer IDs printed (6 and 167, per design-time probing), no errors. Record both — Task 2 needs them.

- [ ] **Step 15: Commit**

```bash
git add requirements.txt requirements-dev.txt pytest.ini collectors/ tests/ .gitignore
git commit -m "discovery: add IGDB client with OAuth token, platform lookup, and paginated game fetch"
```

(Add `.venv/` to `.gitignore` if not already covered.)

---

## Task 2: Coverage classification and game_key collision detection

**Files:**
- Create: `collectors/discovery/build_seed.py`
- Create: `tests/discovery/test_build_seed.py`

**Interfaces:**
- Consumes: Task 1's `igdb_client.fetch_games_by_platform` result shape (`{"id", "name", "platforms", "websites"}` dicts).
- Produces (for Task 3):
  - `build_seed.game_key_for(name: str) -> str` — lowercase `name`, collapse non-alphanumeric runs to single underscores, strip leading/trailing underscores (e.g. `"Elden Ring: Deluxe Edition"` → `"elden_ring_deluxe_edition"`).
  - `build_seed.extract_steam_appid(websites: list[str]) -> int | None` — returns the integer from the first URL matching `store.steampowered.com/app/(\d+)`, else `None`.
  - `build_seed.extract_ps_store_path(websites: list[str]) -> str | None` — returns the first match of `store.playstation.com/[^/]+/((?:concept|product)/[^/?]+)` (the group, not the whole URL), else `None`.
  - `build_seed.classify_coverage(has_pc: bool, has_ps5: bool) -> str` — `"BOTH"` if both, `"STEAM_ONLY"` if only `has_pc`, `"PS5_ONLY"` if only `has_ps5`, `"OTHER"` if neither.
  - `build_seed.build_candidates(pc_games: list[dict], ps5_games: list[dict]) -> list[dict]` — for the union of `pc_games` and `ps5_games` (deduplicated by `id`), computes `has_pc = id in {g["id"] for g in pc_games}`, `has_ps5 = id in {g["id"] for g in ps5_games} or extract_ps_store_path(websites) is not None` (the second clause is the backward-compatibility fix — see Global Constraints), classifies coverage, and discards `"OTHER"`. Returns `{"igdb_id": int, "canonical_name": str, "coverage": str, "websites": list[str]}` per kept game.
  - `build_seed.find_game_key_collisions(candidates: list[dict]) -> tuple[list[dict], list[dict]]` — groups candidates by `game_key_for(canonical_name)`; returns `(clean, collided)` where `collided` holds every candidate whose `game_key` is shared by more than one distinct `igdb_id`, and `clean` holds the rest.
  - `build_seed.write_candidates_csv(candidates: list[dict], path: str) -> None` — writes `igdb_id,canonical_name,coverage` with a header row, using the stdlib `csv` module (handles quoting for titles containing commas). `websites` is intentionally omitted (internal-only, not useful to a human reviewer at this checkpoint).

- [ ] **Step 1: Write the failing tests for `game_key_for`**

```python
# tests/discovery/test_build_seed.py
from collectors.discovery.build_seed import game_key_for


def test_game_key_for_slugifies_name():
    assert game_key_for("Elden Ring") == "elden_ring"


def test_game_key_for_strips_punctuation_and_collapses_runs():
    assert game_key_for("Elden Ring: Deluxe Edition!") == "elden_ring_deluxe_edition"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: both FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `game_key_for`**

```python
import re

def game_key_for(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: both PASS.

- [ ] **Step 5: Write the failing tests for `extract_steam_appid` and `extract_ps_store_path`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import game_key_for, extract_steam_appid, extract_ps_store_path
```

```python
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
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all 5 new tests FAIL.

- [ ] **Step 7: Implement `extract_steam_appid` and `extract_ps_store_path`**

```python
STEAM_APPID_RE = re.compile(r"store\.steampowered\.com/app/(\d+)")
PS_STORE_PATH_RE = re.compile(r"store\.playstation\.com/[^/]+/((?:concept|product)/[^/?]+)")


def extract_steam_appid(websites: list[str]) -> int | None:
    ...


def extract_ps_store_path(websites: list[str]) -> str | None:
    ...
```

For each, iterate `websites`, return the first regex match's group (cast to `int` for the appid), else `None`.

- [ ] **Step 8: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all PASS.

- [ ] **Step 9: Write the failing tests for `classify_coverage`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage
```

```python
def test_classify_coverage_both():
    assert classify_coverage(has_pc=True, has_ps5=True) == "BOTH"


def test_classify_coverage_steam_only():
    assert classify_coverage(has_pc=True, has_ps5=False) == "STEAM_ONLY"


def test_classify_coverage_ps5_only():
    assert classify_coverage(has_pc=False, has_ps5=True) == "PS5_ONLY"


def test_classify_coverage_other():
    assert classify_coverage(has_pc=False, has_ps5=False) == "OTHER"
```

- [ ] **Step 10: Run tests to verify they fail**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all 4 FAIL.

- [ ] **Step 11: Implement `classify_coverage`**

Pure function, implements the logic described in Interfaces above.

- [ ] **Step 12: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all PASS.

- [ ] **Step 13: Write the failing tests for `build_candidates`, including the backward-compatibility case**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import (
    game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage, build_candidates,
)
```

```python
def test_build_candidates_classifies_and_discards_other():
    pc_games = [
        {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": ["https://store.steampowered.com/app/1245620"]},
        {"id": 3, "name": "A PC Exclusive", "platforms": [6], "websites": []},
    ]
    ps5_games = [
        {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": ["https://store.steampowered.com/app/1245620"]},
        {"id": 4, "name": "A PS5 Exclusive", "platforms": [167], "websites": []},
    ]

    result = build_candidates(pc_games, ps5_games)

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

    result = build_candidates(pc_games, ps5_games)

    assert result == [
        {
            "igdb_id": 2,
            "canonical_name": "Red Dead Redemption 2",
            "coverage": "BOTH",
            "websites": ["https://store.playstation.com/en-us/concept/209441"],
        },
    ]
```

- [ ] **Step 14: Run tests to verify they fail**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: both new tests FAIL (`build_candidates` not defined).

- [ ] **Step 15: Implement `build_candidates`**

Build `pc_ids = {g["id"] for g in pc_games}` and `ps5_ids = {g["id"] for g in ps5_games}`. For each game in the union of `pc_games` and `ps5_games` (dedupe by `id`, keep the first occurrence's data), compute `has_pc = game["id"] in pc_ids`, `has_ps5 = game["id"] in ps5_ids or extract_ps_store_path(game["websites"]) is not None`, classify via `classify_coverage`, discard `"OTHER"`.

- [ ] **Step 16: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all PASS.

- [ ] **Step 17: Write the failing tests for `find_game_key_collisions`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import (
    game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage, build_candidates,
    find_game_key_collisions,
)
```

```python
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
```

- [ ] **Step 18: Run test to verify it fails**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: FAILS (`find_game_key_collisions` not defined).

- [ ] **Step 19: Implement `find_game_key_collisions`**

Group candidates by `game_key_for(c["canonical_name"])`. For each group, if it contains more than one distinct `igdb_id`, its members go to `collided`; otherwise to `clean`. Preserve input order within each output list.

- [ ] **Step 20: Run test to verify it passes**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: PASSES.

- [ ] **Step 21: Write the failing test for `write_candidates_csv`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import (
    game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage, build_candidates,
    find_game_key_collisions, write_candidates_csv,
)
```

```python
import csv


def test_write_candidates_csv_round_trips_commas_in_titles(tmp_path):
    candidates = [
        {"igdb_id": 1, "canonical_name": "Tom Clancy's, The Division", "coverage": "BOTH", "websites": []},
    ]
    path = tmp_path / "candidates.csv"

    write_candidates_csv(candidates, str(path))

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows == [{"igdb_id": "1", "canonical_name": "Tom Clancy's, The Division", "coverage": "BOTH"}]
```

- [ ] **Step 22: Run test to verify it fails**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: FAILS (`write_candidates_csv` not defined).

- [ ] **Step 23: Implement `write_candidates_csv`**

Use `csv.DictWriter` with `fieldnames=["igdb_id", "canonical_name", "coverage"]`, `writeheader()`, then write each candidate restricted to those three keys (drop `websites`).

- [ ] **Step 24: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all PASS.

- [ ] **Step 25: Commit**

```bash
git add collectors/discovery/build_seed.py tests/discovery/test_build_seed.py
git commit -m "discovery: classify IGDB candidates into three coverage tiers, detect game_key collisions"
```

---

## Task 3: Identifier resolution and Steam fallback matching

**Files:**
- Create: `collectors/discovery/steam_match.py`
- Create: `tests/discovery/test_steam_match.py`
- Modify: `collectors/discovery/build_seed.py` (add `attach_identifiers`, `match_candidates`)
- Modify: `tests/discovery/test_build_seed.py`

**Interfaces:**
- Consumes: Task 2's `clean` candidate dicts (`igdb_id, canonical_name, coverage, websites`).
- Produces (for Task 4):
  - `steam_match.normalize_name(name: str) -> str` — lowercase; strips a fixed list of edition/suffix markers (`": deluxe edition"`, `": complete edition"`, `": remastered"`, `": director's cut"`, `": game of the year edition"` — case-insensitive) and surrounding punctuation/whitespace.
  - `steam_match.search_steam(name: str) -> list[dict]` — `GET https://store.steampowered.com/api/storesearch/?term=<name>&cc=us&l=en`, returns `response.json()["items"]` as `[{"id": int, "name": str}, ...]`.
  - `steam_match.best_match(target_name: str, steam_results: list[dict]) -> tuple[dict | None, bool]` — returns `(None, False)` if `steam_results` is empty; otherwise returns the first result and `True` if its normalized name equals the normalized `target_name`, else the first result and `False`.
  - `build_seed.attach_identifiers(candidates: list[dict]) -> list[dict]` — for each candidate, adds `steam_appid` (via `extract_steam_appid(websites)`, only meaningful for `BOTH`/`STEAM_ONLY`; `None` otherwise or if unresolved) and `ps_store_path` (via `extract_ps_store_path(websites)`, only meaningful for `BOTH`/`PS5_ONLY`; `None` otherwise or if unresolved). Drops the `websites` key (no longer needed downstream).
  - `build_seed.match_candidates(candidates: list[dict]) -> tuple[list[dict], list[dict]]` — candidates are already post-`attach_identifiers`. A candidate is **matched** outright if it needs no `steam_appid` (its coverage is `PS5_ONLY`, or `steam_appid` is already set) **and** it needs no `ps_store_path` (coverage is `STEAM_ONLY`, or `ps_store_path` is already set). Otherwise: if the candidate's coverage needs a `steam_appid` it doesn't have, call `search_steam`/`best_match` as a fallback — on a confident match, fill in `steam_appid` and re-check; if still missing anything required by its coverage after the fallback (an unresolved `steam_appid`, or any missing `ps_store_path` — which has no fallback), the candidate goes to `needs_review` carrying a `steam_candidates` key (the raw Steam results, `[]` if none were fetched) so a human can finish it.

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

Return `(None, False)` on empty input. Otherwise compare `normalize_name(target_name)` to `normalize_name(results[0]["name"])`; return `(results[0], True)` if equal, else `(results[0], False)`.

- [ ] **Step 12: Run tests to verify they pass**

Run: `pytest tests/discovery/test_steam_match.py -v`
Expected: all PASS.

- [ ] **Step 13: Write the failing tests for `build_seed.attach_identifiers`**

First, update the top-of-file import in `tests/discovery/test_build_seed.py`:
```python
from collectors.discovery.build_seed import (
    game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage, build_candidates,
    find_game_key_collisions, write_candidates_csv, attach_identifiers,
)
```

```python
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
```

- [ ] **Step 14: Run test to verify it fails**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: FAILS (`attach_identifiers` not defined).

- [ ] **Step 15: Implement `attach_identifiers`**

For each candidate, set `steam_appid = extract_steam_appid(candidate["websites"])` and `ps_store_path = extract_ps_store_path(candidate["websites"])`, then drop the `websites` key. (Resolving an identifier the coverage doesn't need, e.g. a `ps_store_path` for a `STEAM_ONLY` game, is harmless — Task 4 only reads the fields each coverage actually requires.)

- [ ] **Step 16: Run test to verify it passes**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: PASSES.

- [ ] **Step 17: Write the failing tests for `build_seed.match_candidates`**

First, update the top-of-file import:
```python
from collectors.discovery.build_seed import (
    game_key_for, extract_steam_appid, extract_ps_store_path, classify_coverage, build_candidates,
    find_game_key_collisions, write_candidates_csv, attach_identifiers, match_candidates,
)
```

```python
from unittest.mock import patch


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
```

- [ ] **Step 18: Run tests to verify they fail**

Run: `pytest tests/discovery/test_build_seed.py -v`
Expected: all 4 new tests FAIL (`match_candidates` not defined).

- [ ] **Step 19: Implement `match_candidates`**

Import `search_steam` and `best_match` from `collectors.discovery.steam_match` at module level in `build_seed.py` (so the `patch` target in tests resolves correctly). For each candidate: determine `needs_steam = candidate["coverage"] in ("BOTH", "STEAM_ONLY") and candidate["steam_appid"] is None`. If `needs_steam`, call `search_steam(candidate["canonical_name"])` then `best_match(candidate["canonical_name"], results)`; on a confident match, set `candidate["steam_appid"]` to the match's `id`. After this, a candidate is "complete" if it has no missing field its coverage requires (`steam_appid` for `BOTH`/`STEAM_ONLY`, `ps_store_path` for `BOTH`/`PS5_ONLY`). Complete candidates go to `matched` unchanged (no `steam_candidates` key). Incomplete candidates go to `needs_review` with a `steam_candidates` key added (the raw `search_steam` results if a fallback call was made, else `[]`).

- [ ] **Step 20: Run tests to verify they pass**

Run: `pytest tests/discovery/test_build_seed.py tests/discovery/test_steam_match.py -v`
Expected: all PASS.

- [ ] **Step 21: Commit**

```bash
git add collectors/discovery/steam_match.py tests/discovery/test_steam_match.py collectors/discovery/build_seed.py tests/discovery/test_build_seed.py
git commit -m "discovery: resolve storefront identifiers from IGDB websites, fall back to Steam search"
```

---

## Task 4: Manual review merge and seed file finalization

**Files:**
- Create: `collectors/discovery/finalize_seed.py`
- Create: `tests/discovery/test_finalize_seed.py`

**Interfaces:**
- Consumes: Task 2's `collided` list, Task 3's `matched`/`needs_review` lists.
- Produces (for Task 5):
  - `finalize_seed.merge_reviewed(matched: list[dict], needs_review: list[dict], review_decisions: dict[int, dict | None]) -> list[dict]` — `review_decisions` maps `igdb_id → a dict of corrected fields to merge in` (e.g. `{"steam_appid": 374320}`, or `{"ps_store_path": "concept/12345"}`, or `{"canonical_name": "Resident Evil 4 (2023)"}` for a collision the human disambiguated) **or** `None` (human rejected — game excluded). Returns the combined list: every `matched` entry, plus every `needs_review` entry whose `igdb_id` has a non-`None` decision, with that entry updated by the decision's keys before inclusion (dropping `steam_candidates`). An `igdb_id` with no entry in `review_decisions` at all is treated as still pending and excluded (not silently guessed at).
  - **Orchestration note (not a separate tested function — wiring for the live discovery run):** Task 2's `collided` list has a different shape (`igdb_id, canonical_name, coverage, websites`) than `needs_review` (`..., steam_appid, ps_store_path, steam_candidates`). Before calling `merge_reviewed`, the live run script must first pass `collided` through Task 3's `attach_identifiers` (to populate `steam_appid`/`ps_store_path` from `websites`, then drop `websites`), add a `steam_candidates: []` key to each, and concatenate the result onto `needs_review` — collided entries never go through `match_candidates`'s Steam-search fallback, since a human must pick the correct game before any automated matching is meaningful for it.
  - `finalize_seed.to_dim_game_rows(merged: list[dict], added_date: str) -> list[dict]` — maps each merged dict to the final schema: `game_key` (`build_seed.game_key_for(canonical_name)`), `canonical_name`, `coverage`, `steam_appid`, `ps_store_path`, `igdb_id`, `added_date`.
  - `finalize_seed.write_seed_csv(rows: list[dict], path: str) -> None` — writes with header `game_key,canonical_name,coverage,steam_appid,ps_store_path,igdb_id,added_date`, empty string for any `None` value.

- [ ] **Step 1: Write the failing tests for `merge_reviewed`**

```python
# tests/discovery/test_finalize_seed.py
from collectors.discovery.finalize_seed import merge_reviewed


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: all FAIL.

- [ ] **Step 3: Implement `merge_reviewed`**

Start with a copy of each `matched` entry. For each entry in `needs_review`, look up `decision = review_decisions.get(entry["igdb_id"])`; if `decision` is a dict, merge it into a copy of `entry` (overwriting matching keys), drop `steam_candidates`, and append. If `decision` is `None` or absent, skip.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: all PASS.

- [ ] **Step 5: Write the failing test for `to_dim_game_rows`**

First, update the top-of-file import:
```python
from collectors.discovery.finalize_seed import merge_reviewed, to_dim_game_rows
```

```python
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
```

- [ ] **Step 6: Run test to verify it fails**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: FAILS (`to_dim_game_rows` not defined).

- [ ] **Step 7: Implement `to_dim_game_rows`**

Import `game_key_for` from `collectors.discovery.build_seed`. Build each row as described in Interfaces.

- [ ] **Step 8: Run test to verify it passes**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: PASSES.

- [ ] **Step 9: Write the failing test for `write_seed_csv`**

First, update the top-of-file import:
```python
from collectors.discovery.finalize_seed import merge_reviewed, to_dim_game_rows, write_seed_csv
```

```python
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
```

- [ ] **Step 10: Run test to verify it fails**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: FAILS (`write_seed_csv` not defined).

- [ ] **Step 11: Implement `write_seed_csv`**

`csv.DictWriter` with the fixed fieldnames from Interfaces; `None` values write as empty strings automatically.

- [ ] **Step 12: Run tests to verify they pass**

Run: `pytest tests/discovery/test_finalize_seed.py -v`
Expected: all PASS.

- [ ] **Step 13: Commit**

```bash
git add collectors/discovery/finalize_seed.py tests/discovery/test_finalize_seed.py
git commit -m "discovery: merge human review decisions (including collisions) and finalize the dim_game seed CSV"
```

---

## Task 5: Load the seed into an Iceberg `dim_game` table

**Files:**
- Create: `infra/spark/jobs/load_dim_game.py`
- Create: `infra/spark/jobs/fixtures/dim_game_seed_fixture.csv`
- Modify: `docker-compose.yml` (mount `./infra/spark/jobs` and a `data/` directory into `spark-master`, if not already covered by Plan 1's mounts)

**Interfaces:**
- Consumes: a seed CSV matching Task 4's schema, at a path passed via a positional argument.
- Produces: an Iceberg table `lakehouse.dim.dim_game`, fully replaced (not appended) on every run — the same truncate-and-reload reasoning as the spec's ClickHouse sync, since `dim_game` is small and meant to be reloaded whenever the seed file changes, not accumulated into.

- [ ] **Step 1: Add a fixture seed CSV covering all three coverage tiers, independent of real discovery data**

`infra/spark/jobs/fixtures/dim_game_seed_fixture.csv`:
```csv
game_key,canonical_name,coverage,steam_appid,ps_store_path,igdb_id,added_date
elden_ring,Elden Ring,BOTH,1245620,concept/10000333,119133,2026-11-05
hollow_knight,Hollow Knight,STEAM_ONLY,367520,,999,2026-11-05
a_ps5_exclusive,A PS5 Exclusive,PS5_ONLY,,concept/999999,888,2026-11-05
```

This fixture proves the loading mechanism works independently of whether Tasks 1-4's live discovery run has happened yet, and specifically exercises the `PS5_ONLY` row (null `steam_appid`) that the previous version of this plan couldn't produce — same reasoning as Plan 1's `smoke_test.py` using synthetic data rather than real business data.

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
    StructField("ps_store_path", StringType(), True),
    StructField("igdb_id", IntegerType(), False),
    StructField("added_date", StringType(), False),
])


def main(seed_path: str) -> None:
    spark = SparkSession.builder.appName("load-dim-game").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dim")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.dim.dim_game (
            game_key STRING, canonical_name STRING, coverage STRING,
            steam_appid INT, ps_store_path STRING, igdb_id INT, added_date DATE
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
Expected: stdout includes `LOAD DIM_GAME PASSED: 3 rows`.

- [ ] **Step 5: Run the load job a second time and verify idempotency (Review Focus item)**

Run the same command from Step 4 again.
Expected: stdout again includes `LOAD DIM_GAME PASSED: 3 rows` — not 6. `INSERT OVERWRITE` replaces the table's contents rather than appending, so re-running with the same fixture must not double the row count.

- [ ] **Step 6: Query the loaded table and confirm the `PS5_ONLY` row's `steam_appid` is genuinely NULL (not the string "None" or empty string misread as a value)**

Run: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 -e "spark.sql('SELECT game_key, steam_appid FROM lakehouse.dim.dim_game WHERE coverage = \"PS5_ONLY\"').show()"` (or an equivalent one-off `spark-sql`/Python snippet if `-e` isn't supported by this Spark distribution's `spark-submit` — check `spark-submit --help` first if it fails, and use `pyspark` or a small inline script instead)
Expected: `steam_appid` column shows `NULL` for the `a_ps5_exclusive` row.

- [ ] **Step 7: Commit**

```bash
git add infra/spark/jobs/
git commit -m "discovery: add Iceberg dim_game load job for the three-tier schema, verified idempotent"
```

---

## What's left to the builder / explicitly deferred

- **The actual live discovery run** (Tasks 1-4 run for real, against real IGDB data, producing the real `data/dim_game_seed.csv`) is a manual step the project owner runs once this plan is implemented — this plan delivers the tooling, not a populated seed file.
- **PS5 price collection itself** (actually fetching current prices from the PlayStation Store page for each `dim_game` row with a `ps_store_path`) is a separate, later plan — this plan only resolves *identity* (which game maps to which PS Store URL), not price facts.
- **Airflow orchestration of the load job** is deferred to Plan 9 per the Plan 1 review's explicit ruling (Airflow has no Spark/Java in its image; solving that generically belongs with the batch-chain DAG design, not bolted on here for one job).
- The exact number of `BOTH`/`STEAM_ONLY`/`PS5_ONLY` games this produces is whatever IGDB's real data yields — the spec's `M`/`y` tier-sizing formula depends on the verified mutual count `M`, which isn't final until the live discovery run completes.
