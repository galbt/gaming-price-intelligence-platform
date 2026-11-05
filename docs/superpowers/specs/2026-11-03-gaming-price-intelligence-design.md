# GameDeal Intelligence — Design Document

Date: 2026-11-03
Repo: galbt/gaming-price-intelligence-platform

## Intent and why

This is a personal learning project. The primary goal is hands-on,
end-to-end experience with Kafka, Spark, and Iceberg/Parquet specifically,
plus a self-hosted columnar warehouse and an orchestrator — not a specific
business outcome. The project must be real enough (real data, real
failure modes) to be a credible portfolio piece, runnable entirely on a
laptop, and pushed to a private GitHub repo.

To make the tooling exercise meaningful rather than arbitrary, the project
is framed around a genuine consumer problem: **GameDeal Intelligence** — a
platform that tracks real game prices across PC (Steam) and PS5 (direct
PlayStation Store price scraping — see Data sources), stores full price
history, and answers "is this actually a good deal, and should I buy now
or wait?"

## Goals

- Build a working, end-to-end local pipeline: Kafka → Spark (streaming +
  batch) → Iceberg (Bronze/Silver/Gold) → ClickHouse, orchestrated by
  Airflow, entirely in docker-compose.
- Track real price/discount data for a curated watchlist of 350 games
  (split across mutual and platform-exclusive tiers — see Scope),
  comparing PC (Steam) vs PS5 (PlayStation Store) pricing.
- Produce a Deal Quality Score (0-100) per **(game, platform, region)**,
  based on that combination's own historical price behavior (historical
  low, typical discount depth, sale frequency) — not just the headline
  discount percentage. A single blended per-game score can't answer
  "where's it cheaper," so Steam and PS5 — and IL vs. US — each get their
  own score, with a simple "best current deal" rollup for Gold's headline
  ranking. (Settled by the project owner/author during design; the exact
  formula weights are left to the builder.)
- Support the MVP consumer questions:
  - Is this a good deal right now? (Deal Quality Score)
  - Where's it cheaper — Steam or PS5? (for mutual titles)
  - What's this game's price history?
  - What are the best deals right now, across the watchlist?
- Support broader, platform-level questions enabled by also tracking
  platform-exclusive titles (see Scope): does one platform discount more
  aggressively than the other overall? Do exclusives get better or worse
  deals than multiplatform titles? Are there genre-level discount
  patterns that differ by platform?
- Exercise the harder, more realistic capabilities of each tool rather
  than the trivial path: Spark Structured Streaming with Kafka, Iceberg
  `MERGE INTO` for real change-detection (not just appends), Airflow DAG
  dependencies/retries/backfill, ClickHouse reading Iceberg natively.

## Non-goals

- No PS4, Xbox, or Nintendo tracking. Console makers don't expose public
  pricing/usage APIs in general, and for this project specifically we
  scoped PlayStation to PS5 only.
- No multi-edition, DLC, or bundle tracking. One edition per game per
  platform (standard edition), chosen for comparability, not completeness.
- No fuzzy-matching automation left unchecked: catalog discovery is
  API-driven (see Scope), but ambiguous matches (sequels, editions,
  remasters) always get a human review pass before entering `dim_game` —
  nothing is auto-accepted on a low-confidence match. This matters
  concretely, not just in principle: live probing during design found
  IGDB listing the 2005 original *Resident Evil 4* and the 2023 remake
  under the identical title "Resident Evil 4" — exact-name matching alone
  cannot disambiguate that case.
- No cloud deployment of any kind (no AWS, no managed services, no company
  infrastructure). Docker-compose on a laptop only.
- No BI tool in the MVP. Presentation is direct SQL against ClickHouse;
  Metabase/Superset is an explicit future add-on, not core scope.
- No price alerting or predictive "buy now vs. wait" modeling in the MVP
  (P1/P2 in the original brainstorm — may come later, not designed here).

## Anti-goals

(Things that would make this a failure even if it technically runs.)

- A pipeline that only "works" because it ignores the real fragility of
  its sources — e.g. treating Steam's unofficial `appdetails` endpoint as
  if it were stable and documented. It isn't; the pipeline must handle
  missing fields, request failures, and format drift without falling
  over.
- Treating the PlayStation Store price scrape (see Data sources) as if it
  were a stable, documented API. It isn't: it's an unauthenticated HTML
  page fetch of a schema.org `Product`/`Offer` block Sony could restructure
  without notice. The collector must fail gracefully (log and skip, not
  crash the DAG) if the page shape changes, same posture as Steam's
  unofficial endpoint.
- Trusting IGDB's `platforms` field as the sole signal for PS5
  availability. Live probing found *Red Dead Redemption 2* missing
  `PlayStation 5` from its `platforms` list despite having a live, working
  PS Store listing (it's sold via PS4 backward compatibility, not a native
  PS5 SKU) — the pipeline must treat "a working PS Store URL resolves" as
  the real availability signal, not the platform tag alone.
- A pipeline that uses Kafka/Spark/Iceberg/Airflow as decoration rather
  than for what they're actually good at — e.g. Bronze landing as a
  trivial append-only dump with no real Silver change-detection logic, or
  Airflow DAGs with no real dependency chain. The point of the project is
  depth with these tools, not their presence in a compose file.

## Constraints

- Runs fully locally via docker-compose. No cloud dependency of any kind.
- Must use: Kafka, Spark, Iceberg (on Parquet), a self-hosted columnar
  warehouse (ClickHouse), and Airflow (decided explicitly, built in from
  the start rather than added later).
- PlatPrices is **not used** — confirmed as of 2026-11-05 to require a
  paid purchase (no free-tier key available), contrary to the original
  plan. PS5 pricing instead comes from a direct, unauthenticated
  PlayStation Store page fetch (see Data sources) — no quota, no key,
  no cost, but no stability guarantee either (see Anti-goals/Risks).
- Steam's pricing endpoint (`store.steampowered.com/api/appdetails`) is
  unofficial and undocumented. No official rate limit, but no stability
  guarantee either — must be polled defensively.
- IGDB (via Twitch developer OAuth, free, no payment) replaces RAWG for
  discovery/metadata as of 2026-11-05 — RAWG suffered an extended outage
  (Cloudflare 522, origin unreachable across multiple checks spanning
  more than a day) during this project's build. IGDB's free tier
  (~4 req/sec, 8 concurrent, no published monthly cap) is not a binding
  constraint for a 350-game watchlist.

## Data sources

| Source | Role | Auth | Budget constraint |
|---|---|---|---|
| Steam `appdetails` (`price_overview`) | PC price facts: currency, initial/final price, discount % | None (keyless, unofficial) | None official — self-imposed politeness only |
| PlayStation Store page fetch (`store.playstation.com/<locale>/{concept,product}/<id>`) | PS5 price facts: current price, discount %, pre-order vs. live status (standard edition only) | None (keyless, unauthenticated) | None official — self-imposed politeness only. Not PlatPrices: PlatPrices was confirmed paid-only on 2026-11-05 and is not used anywhere in this design. |
| IGDB (`api.igdb.com`, via Twitch OAuth) | Dimension/metadata: platforms, release date, publisher; **primary** discovery/matching source — `websites` field gives direct Steam and PS Store URLs per game, not just a name to fuzzy-match | Free Twitch developer app (`client_id`/`client_secret`, OAuth2 client-credentials) | ~4 req/sec, 8 concurrent — not binding for 350 games |
| Steam `storesearch` (unofficial) | **Fallback** matching: resolves a game to its Steam listing when IGDB's `websites` field omits the Steam URL (confirmed happens — e.g. *Marvel's Spider-Man Remastered*) | None (keyless, unofficial) | None official — self-imposed politeness only |
| IsThereAnyDeal (ITAD) | One-time/periodic **backfill**: historical low + price history for the PC/Steam side only, used to seed Silver/Gold with real history on day one instead of waiting weeks | Free API key | Batches up to 50 games/call; exact rate limits unconfirmed — verify live before relying on it (see Risks) |

Note: ITAD backfill covers the **PC side only**. There is no free
historical-price source for PS5 — the PlayStation Store page fetch only
ever returns the *current* price, same as PlatPrices' price-history
endpoint would have been paid-only anyway — so per the project owner's
decision, PS5 history builds up organically through live collection;
expect it to take roughly a month before the Deal Quality Score on that
side is meaningful.

**On matching reliability (found via live probing during design, 12 test
titles across both console-exclusive and multi-edition franchises):**
IGDB correctly excludes non-mutual titles (e.g. *Gears of War* — no PS5 in
`platforms`, correctly bucketed away from `BOTH`) and resolves direct
storefront URLs for most clean single-edition games (*God of War
Ragnarök*, *Elden Ring*, *Cyberpunk 2077*, *Hogwarts Legacy*, *Baldur's
Gate III*, *Mortal Kombat 1*, *Diablo IV*, *Horizon Forbidden West*).
IGDB is the **primary** source for discovery and matching, not a
drop-in oracle — three concrete failure modes to design around:
1. **Sequel/remake name collisions** (see Anti-goals) — needs the
   existing human-review gate, not just exact-name matching.
2. **`platforms` misses backward-compatible availability** (see
   Anti-goals) — use PS Store URL resolution as the real signal.
3. **Incomplete `websites` data** — IGDB is community-sourced and
   sometimes omits a storefront link that genuinely exists (confirmed:
   *Marvel's Spider-Man Remastered* had a PS Store URL but no Steam URL).
   Steam's own `storesearch` is kept as a fallback for exactly this case.

## Scope

- **Watchlist: 350 games total, split across three coverage tiers**:
  - **Mutual (`BOTH`)**: on both Steam (PC) and PS5, standard edition —
    the count here, `M`, is determined by how many titles the discovery
    process (below) can actually find and verify, not fixed in advance.
    An initial set of 20 candidate titles was already selected and
    partly verified during brainstorming (a mix of long-running titles
    with deep discount history and recent releases) and seeds this tier.
  - **Steam-exclusive (`STEAM_ONLY`)**: on PC only. Count: `y`.
  - **PS5-exclusive (`PS5_ONLY`)**: on PS5 only. Count: `y`.
  - Where `y = (350 - M) / 2` — the two exclusive tiers are kept equal in
    size, for a balanced platform-level comparison.
  - These exclusive tiers exist specifically to support the broader
    platform-level questions in Goals (discount strategy by platform,
    exclusives vs. multiplatform) — they can't participate in
    item-level cross-platform comparison the way `BOTH` games can.

- **Discovery process** (build-time, produces the `dim_game` seed data):
  query IGDB by platform to classify each candidate's coverage tier — PC
  **and** PS5 (candidates for `BOTH`), PC **not** PS5 (`STEAM_ONLY`), PS5
  **not** PC (`PS5_ONLY`) — treating a resolvable PS Store URL in IGDB's
  `websites` field as confirmation of real PS5 availability (not the
  `platforms` tag alone — see Anti-goals). IGDB's `websites` field
  supplies the Steam URL directly for most titles; when it's missing,
  fall back to Steam's `storesearch` to resolve it. Clean, unambiguous
  title matches (exact name match against IGDB's result, one unambiguous
  storefront URL per platform) are auto-accepted; anything ambiguous
  (sequels/remakes sharing an identical title, "Complete Edition" vs.
  base game, multiple IGDB entries with no clear canonical pick) goes
  through a manual review pass before being added. The output is still a
  version-controlled seed file in the repo (CSV/YAML) — generated rather
  than hand-typed, but just as editable/correctable afterward.

- **Identity resolution / `dim_game`**: loaded from the seed file into
  Iceberg by a batch job. Schema:

  ```
  dim_game (
    game_key          STRING,   -- canonical id, e.g. "elden_ring"
    canonical_name    STRING,
    coverage          STRING,   -- "BOTH" | "STEAM_ONLY" | "PS5_ONLY"
    steam_appid       INT,      -- null if coverage = "PS5_ONLY"
    ps_store_path     STRING,   -- null if coverage = "STEAM_ONLY"; the
                                 -- trailing URL segment after locale, e.g.
                                 -- "concept/10000333" or
                                 -- "product/EP1004-...-STANDARD001"; the
                                 -- collector builds the fetch URL as
                                 -- store.playstation.com/<locale>/<this>
    igdb_id           INT,
    added_date        DATE
  )
  ```

  `coverage` drives which collector(s) poll a given game (a
  `STEAM_ONLY` row is never sent to the PS Store collector, and vice
  versa), and it's the join key for every cross-platform comparison in
  Silver/Gold — Steam facts arrive keyed by `steam_appid`, PS Store facts
  by `ps_store_path`; `dim_game` is what makes `BOTH`-coverage games
  comparable, while `STEAM_ONLY`/`PS5_ONLY` games feed the platform-level
  aggregates instead.

## Regions and currency

Both Steam and the PlayStation Store are queried in **two fixed regions**:
Israel (the project owner's home region, `IL`) and the United States
(`US`). Steam supports this via its `cc` query parameter directly; the
PlayStation Store supports it via the locale segment in the page path —
confirmed live during design: `store.playstation.com/en-us/concept/<id>`
returns USD pricing, `store.playstation.com/en-il/concept/<id>` returns
ILS pricing, same concept ID, same product, no separate region parameter
or lookup needed. Each collector only queries the regions for games
actually in its scope per `dim_game.coverage` (see Scope) — e.g. a
`STEAM_ONLY` game never generates a PS Store fetch in either region.

Every fact row (Steam and PS Store) is stored with its **region** and
**native currency** as collected, for fidelity. For comparison purposes
(the "where's it cheaper" and Deal Quality Score questions), Gold
converts everything to **USD**, using a small reference table:

```
fx_rates (
  currency      STRING,   -- e.g. "ILS"
  rate_to_usd   DOUBLE,
  as_of_date    DATE
)
```

Refreshed weekly (same cadence as RAWG) from a free, keyless FX API
(e.g. Frankfurter.app). The `US` region is already natively USD, so this
table mainly matters for converting the `IL` (ILS) leg.

This means fact and Gold tables carry a `(game_key, platform, region)`
grain, not just `(game_key, platform)` — see the Deal Quality Score note
below.

## Polling cadence

Both price sources poll **daily** (not more often): real discounts
rotate at most about once a day on Steam, and PS Store sale campaigns
run for days to weeks once started, so daily polling catches essentially
every real price change — more frequent polling would mostly produce
redundant "no change" observations, so the budget is better spent on
watchlist breadth than on freshness.

| Source | Games polled | Cadence | Why |
|---|---|---|---|
| Steam | `BOTH` + `STEAM_ONLY` = `M + y` games, per region (IL + US) | Daily | Not budget-constrained, but no real-world benefit to polling more often than discounts actually change. |
| PlayStation Store page fetch | `BOTH` + `PS5_ONLY` = `M + y` games, per region (IL + US), one page fetch per game per region | Daily | No metered quota (unauthenticated page fetch, not a billed API) — unlike PlatPrices, there's no request budget to derive or stay under. Self-imposed politeness (request spacing) applies instead, same posture as the Steam collector. |
| IGDB | All 350 games | Weekly | Metadata for already-released games is effectively static; weekly is generous freshness margin at negligible API cost (~4 req/sec budget, not binding). |
| ITAD (backfill) | `BOTH` + `STEAM_ONLY` = `M + y` games (PC side only) | One-time initial load, re-synced periodically (e.g. monthly) | It's historical data by nature — no need for frequent polling once the initial backfill lands. |

Note: both collectors end up polling the same number of games (`M + y`)
by construction, since the two exclusive tiers are equal-sized — they
just poll different, overlapping-at-`BOTH` subsets of the 350-game list.

## Architecture

```
Airflow DAGs                  Kafka                 Spark                          Iceberg (on SeaweedFS S3) ClickHouse
┌──────────────────┐   ┌──────────────────┐  ┌──────────────────────┐      ┌─────────────────────┐    ┌──────────────┐
│ steam_collector   │──▶│raw.steam.prices  │─▶│                      │Bronze▶│ bronze.* (append)    │    │              │
│ (daily, batched)  │   ├──────────────────┤  │ Bronze: continuous    │      ├─────────────────────┤    │              │
│ ps_store_coll.    │──▶│raw.ps_store      │─▶│ Structured Streaming  │      │ dim_game (seed load)  │    │              │
│ (daily, page fetch│   ├──────────────────┤  │ (long-running service,│Silver▶│ silver.* (MERGE,      │sync▶│  ClickHouse  │
│ igdb_collector    │──▶│raw.igdb.metadata │─▶│ outside Airflow)       │      │  change events)       │    │  (MergeTree) │
│ (weekly)          │   └──────────────────┘  │                      │Gold──▶│ gold.* (deal score,   │    │              │
└──────────────────┘                          │ Silver/Gold/sync:     │      │  history, comparisons) │    │              │
┌──────────────────┐                          │ batch jobs, Airflow-  │      └─────────────────────┘    └──────────────┘
│ batch_pipeline    │────────────────────────▶│ triggered, dependent  │
│ DAG (Silver→Gold  │                          └──────────────────────┘
│ →CH sync)         │
└──────────────────┘
```

**Kafka** — Apache Kafka in KRaft mode (no Zookeeper), single broker.
Topics `raw.steam.prices`, `raw.ps_store.prices`, `raw.igdb.metadata`,
keyed by `game_key`, 1-3 partitions each, short retention (3-7 days —
Kafka is a replay buffer, not permanent storage; Iceberg is permanent
storage). Kafka UI included for topic/message/consumer-lag visibility
during development.

**Spark** — Standalone cluster (1 master + 2 workers, laptop-sized: 1-2
cores / ~2GB RAM per worker). Two distinct job types:
- **Bronze ingestion**: one continuous Structured Streaming job per topic
  (or a unified job), appending raw records verbatim to Bronze Iceberg
  tables. Runs as its own persistent service, outside Airflow's
  scheduling model (Airflow doesn't manage long-running streaming jobs
  well).
- **Silver/Gold/sync**: batch Spark jobs, triggered as dependent tasks in
  an Airflow DAG. Silver does change-detection via Iceberg `MERGE INTO`
  (only real price changes become events, not every repeated
  observation). Gold computes price history series, the Deal Quality
  Score, and cross-platform comparisons.

Spark UI exposed (8080 master, 4040 per-application) for observability.

**Iceberg** — table format on Parquet files stored in SeaweedFS's S3
emulation (local S3-compatible object storage; chosen over MinIO, whose
free Docker images were pulled from Docker Hub and whose open-source
distribution was discontinued in 2026, and over LocalStack, whose
Community edition cannot persist data across container restarts — see
the infra-foundation plan's ledger for the full story on both swaps).
Catalog: Iceberg REST catalog backed by
Postgres (the same Postgres instance also backs Airflow's metadata DB,
in separate schemas, to limit the number of containers on a laptop).
Bronze/Silver/Gold layering, plus the `dim_game` dimension table.

**ClickHouse** — the serving warehouse. Gold Iceberg tables are synced
into native ClickHouse `MergeTree` tables via ClickHouse's built-in
Iceberg table function reading directly from SeaweedFS's S3 emulation
(`INSERT INTO ...
SELECT * FROM iceberg(...)`), run as the final task in the batch DAG.
Chosen over Snowflake/OpenSearch-style simulation as the most realistic
"runs great in one container on a laptop" option.

Sync strategy is **truncate-and-reload**, not incremental append: each
run, ClickHouse's copy of each Gold table is cleared and reloaded in
full from Iceberg's current state. Gold's tables are cumulative (every
historical observation retained), so a naive incremental append without
a watermark would re-insert the full history on top of itself every run,
duplicating it daily. Truncate-and-reload can't produce that bug, and at
this data volume (350 games, a handful of rows/game/day) the "wasted"
re-read is immaterial. A true incremental sync (tracking the last-synced
Iceberg snapshot ID, appending only new rows) is a reasonable v2 upgrade
and a good excuse to exercise Iceberg's incremental-read support, but
isn't needed for the MVP.

**Airflow** — built in from the start (not deferred), because the batch
chain (Silver → Gold → ClickHouse sync) is a genuine dependency chain
that benefits from real retries, failure alerting, and backfill support,
and because it's a commonly paired skill with Kafka/Spark in practice.
Webserver + scheduler + shared Postgres metadata DB. DAGs:
- Three collector DAGs (`steam_collector`, `ps_store_collector`,
  `igdb_collector`), one per source, each on its own schedule, each
  looping over both regions (IL, US) where applicable.
- One `itad_backfill` DAG — runs once on initial setup, then re-syncs
  periodically (e.g. monthly) to keep PC-side historical data current.
- One `dim_game` load DAG (loads the seed file into Iceberg; run
  on-demand or whenever the seed file changes).
- One `fx_rates` load DAG — weekly, same cadence as IGDB.
- One batch-chain DAG (`silver_gold_sync`) with Silver → Gold → ClickHouse
  sync as dependent tasks, scheduled daily. Gold applies the `fx_rates`
  table to normalize all prices to USD before computing the Deal Quality
  Score and cross-region/cross-platform comparisons.

**Presentation** — MVP is direct SQL against ClickHouse. No BI tool in
scope; Metabase/Superset is a plausible future add-on.

## Risks / things to validate early, before relying on them architecturally

1. **PlayStation Store page structure could change without notice.**
   Confirmed working during design (both `en-us` and `en-il` locales,
   both a `/concept/<id>` and a `/product/<sku>` path style, including a
   real pre-order listing — *GTA VI* at ₪319.00), via a clean embedded
   `application/ld+json` `Product`/`Offer` block plus a richer inline
   data block for discount/edition/platform fields. But it's an
   unauthenticated scrape of Sony's own storefront, not a documented API
   — no stability guarantee. The collector must fail gracefully (log and
   skip, not crash the DAG) if the page shape changes, same posture as
   Steam's unofficial endpoint.
2. **IGDB's `websites` field completeness is not guaranteed.** Confirmed
   gap during design: *Marvel's Spider-Man Remastered* had a PS Store URL
   but no Steam URL in IGDB's data, despite definitely having a Steam
   listing. Steam's own `storesearch` fallback (see Data sources) exists
   specifically to cover this; relying on IGDB alone for Steam-side
   matching will silently under-match some titles.
3. **Sequel/remake title collisions in IGDB.** Confirmed during design:
   the 2005 original *Resident Evil 4* and the 2023 remake share the
   identical IGDB title "Resident Evil 4." Exact-name matching cannot
   disambiguate this on its own — the human review gate (see Scope) must
   catch it, and the discovery process should flag same-title multi-match
   cases for review rather than silently picking the first/highest result.
4. **ITAD's actual free-tier rate limits and exact endpoint paths**
   weren't confirmed from official docs — check github.com/isthereanydeal/api
   before building the backfill job.
5. **Steam's unofficial endpoints** (both `appdetails` and `storesearch`)
   could change or disappear without notice. Collectors should fail
   gracefully (log and skip, not crash the DAG) if their shape changes.

## What's left to the builder

- Running the three-tier discovery process against IGDB/Steam/PlayStation
  Store to actually populate `dim_game` (20 `BOTH`-tier titles are already
  selected from brainstorming and seed that tier; the rest, and the
  `STEAM_ONLY`/`PS5_ONLY` tiers, come from the discovery process defined
  in Scope, including the manual-review pass for ambiguous matches).
- Exact Kafka message schemas (JSON payload shape) per source.
- The Deal Quality Score formula — a reasonable first version (weighted
  combination of distance from historical low, typical discount depth,
  and sale frequency) should be proposed during planning and iterated on
  once real data exists, rather than finalized speculatively here. The
  platform-level aggregate formulas (e.g. "average discount depth by
  platform/coverage tier") are likewise left to planning.
- Iceberg REST catalog image choice and exact Postgres schema layout.
- Airflow DAG file structure and task definitions.
- Choice of Kafka UI vs. Redpanda Console for observability (either is
  fine; Kafka UI was mentioned as the default, no strong reason to prefer
  one).
- Iceberg catalog style: REST+Postgres (default assumed here, simplest)
  vs. Nessie (adds a web UI for table history/branches, more moving
  parts) — Nessie is a reasonable swap if the builder or user wants the
  extra visibility, not required.
- docker-compose resource limits, tuned to the actual machine this runs
  on.
- Kafka topic retention window and Iceberg table compaction/retention
  policy specifics.
