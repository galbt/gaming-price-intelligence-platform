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
platform that tracks real game prices across PC (Steam) and PS5
(PlatPrices), stores full price history, and answers "is this actually a
good deal, and should I buy now or wait?"

## Goals

- Build a working, end-to-end local pipeline: Kafka → Spark (streaming +
  batch) → Iceberg (Bronze/Silver/Gold) → ClickHouse, orchestrated by
  Airflow, entirely in docker-compose.
- Track real price/discount data for a curated watchlist of 100 games,
  comparing PC (Steam) vs PS5 (PlatPrices) pricing.
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
  - Where's it cheaper — Steam or PS5?
  - What's this game's price history?
  - What are the best deals right now, across the watchlist?
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
- No automated cross-catalog entity resolution (fuzzy matching game titles
  across Steam/PlatPrices/RAWG). Identity mapping is hand-curated.
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
- Quietly exceeding the PlatPrices free-tier quota (1,000 requests/month)
  and getting rate-limited or locked out. The batch endpoint is central to
  the design specifically to avoid this — if it doesn't work as assumed
  on the free tier (see Risks below), the request budget must be
  re-derived before relying on it.
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
- PlatPrices free tier: 1,000 requests/month, 120 req/min burst, 2
  regions, no price-history endpoint (not needed — the pipeline builds
  its own history via repeated observation).
- Steam's pricing endpoint (`store.steampowered.com/api/appdetails`) is
  unofficial and undocumented. No official rate limit, but no stability
  guarantee either — must be polled defensively.
- RAWG free tier (~20k requests/month) is not a binding constraint given
  how rarely game metadata changes.

## Data sources

| Source | Role | Auth | Budget constraint |
|---|---|---|---|
| Steam `appdetails` (`price_overview`) | PC price facts: currency, initial/final price, discount % | None (keyless, unofficial) | None official — self-imposed politeness only |
| PlatPrices `/games/batch` | PS5 price facts: current price, discount %, historical low, PS Plus price (standard edition only) | API key | 1,000 req/month free tier — **must use batching** (see below) |
| RAWG | Dimension/metadata: genre, publisher, developer, release date, platforms, ratings | API key | ~20k req/month free tier — not binding |
| IsThereAnyDeal (ITAD) | One-time/periodic **backfill**: historical low + price history for the PC/Steam side only, used to seed Silver/Gold with real history on day one instead of waiting weeks | Free API key | Batches up to 50 games/call; exact rate limits unconfirmed — verify live before relying on it (see Risks) |

Note: ITAD backfill covers the **PC side only**. There is no free historical-price source for PS5 (PlatPrices' price-history endpoint is paid-only), so per the project owner's decision, PS5 history builds up organically through live collection — expect it to take roughly a month before the Deal Quality Score on that side is meaningful.

## Scope

- **Watchlist: 100 games**, hand-curated, tracked on PC (Steam, standard
  edition) and PS5 (PlatPrices, standard edition) only. An initial set of
  20 candidate titles was selected during brainstorming (mix of
  long-running titles with deep discount history, and recent releases);
  expanding to 100 is a build-time curation task, not enumerated here.
- **Identity resolution**: a hand-maintained crosswalk, not automated
  matching. Stored as a version-controlled seed file in the repo (CSV or
  YAML), loaded into an Iceberg `dim_game` table by a batch job. Schema:

  ```
  dim_game (
    game_key          STRING,   -- canonical id, e.g. "elden_ring"
    canonical_name    STRING,
    steam_appid       INT,
    platprices_ppid   STRING,   -- PS5, standard edition
    rawg_id           INT,
    added_date        DATE
  )
  ```

  This table is the join key for every cross-platform comparison in
  Silver/Gold — Steam facts arrive keyed by `steam_appid`, PlatPrices
  facts by `ppid`; `dim_game` is what makes them comparable.

## Regions and currency

Both Steam and PlatPrices are queried in **two fixed regions**: Israel
(the project owner's home region, `IL`) and the United States (`US`).
Steam supports this via its `cc` query parameter directly; PlatPrices
supports it via its `region` parameter, within its free-tier limit of 2
regions — matching exactly. (Whether PlatPrices' region list uses the
code `IL` needs a live check against its `/regions` endpoint — see
Risks.)

Every fact row (Steam and PlatPrices) is stored with its **region** and
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

| Source | Cadence | Why |
|---|---|---|
| Steam | Every 4-6h, per region (IL + US) | Not budget-constrained. Driven by how often real discounts actually rotate (roughly daily, with occasional flash sales) — polling much more often just produces redundant "no change" observations. |
| PlatPrices | Daily, via `/games/batch`, per region (IL + US) | Budget-constrained: PlatPrices charges one quota unit per call regardless of how many records it returns, and the free tier pages at 25 records/page. For 100 games that's `ceil(100/25) = 4` batch calls per region per day → 8 calls/day ≈ 240/month — still well inside the 1,000/month free tier. |
| RAWG | Weekly | Metadata for already-released games is effectively static; weekly is generous freshness margin at negligible API cost. |
| ITAD (backfill) | One-time initial load, re-synced periodically (e.g. monthly) | It's historical data by nature — no need for frequent polling once the initial backfill lands. |

## Architecture

```
Airflow DAGs                  Kafka                 Spark                          Iceberg (on MinIO)          ClickHouse
┌──────────────────┐   ┌──────────────────┐  ┌──────────────────────┐      ┌─────────────────────┐    ┌──────────────┐
│ steam_collector   │──▶│raw.steam.prices  │─▶│                      │Bronze▶│ bronze.* (append)    │    │              │
│ (every 4-6h)      │   ├──────────────────┤  │ Bronze: continuous    │      ├─────────────────────┤    │              │
│ platprices_coll.  │──▶│raw.platprices    │─▶│ Structured Streaming  │      │ dim_game (seed load)  │    │              │
│ (daily, batched)  │   ├──────────────────┤  │ (long-running service,│Silver▶│ silver.* (MERGE,      │sync▶│  ClickHouse  │
│ rawg_collector    │──▶│raw.rawg.metadata │─▶│ outside Airflow)       │      │  change events)       │    │  (MergeTree) │
│ (weekly)          │   └──────────────────┘  │                      │Gold──▶│ gold.* (deal score,   │    │              │
└──────────────────┘                          │ Silver/Gold/sync:     │      │  history, comparisons) │    │              │
┌──────────────────┐                          │ batch jobs, Airflow-  │      └─────────────────────┘    └──────────────┘
│ batch_pipeline    │────────────────────────▶│ triggered, dependent  │
│ DAG (Silver→Gold  │                          └──────────────────────┘
│ →CH sync)         │
└──────────────────┘
```

**Kafka** — Apache Kafka in KRaft mode (no Zookeeper), single broker.
Topics `raw.steam.prices`, `raw.platprices.prices`, `raw.rawg.metadata`,
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

**Iceberg** — table format on Parquet files stored in MinIO (local
S3-compatible object storage). Catalog: Iceberg REST catalog backed by
Postgres (the same Postgres instance also backs Airflow's metadata DB,
in separate schemas, to limit the number of containers on a laptop).
Bronze/Silver/Gold layering, plus the `dim_game` dimension table.

**ClickHouse** — the serving warehouse. Gold Iceberg tables are synced
into native ClickHouse `MergeTree` tables via ClickHouse's built-in
Iceberg table function reading directly from MinIO (`INSERT INTO ...
SELECT * FROM icebergS3(...)`), run as the final task in the batch DAG.
Chosen over Snowflake/OpenSearch-style simulation as the most realistic
"runs great in one container on a laptop" option.

Sync strategy is **truncate-and-reload**, not incremental append: each
run, ClickHouse's copy of each Gold table is cleared and reloaded in
full from Iceberg's current state. Gold's tables are cumulative (every
historical observation retained), so a naive incremental append without
a watermark would re-insert the full history on top of itself every run,
duplicating it daily. Truncate-and-reload can't produce that bug, and at
this data volume (100 games, a handful of rows/game/day) the "wasted"
re-read is immaterial. A true incremental sync (tracking the last-synced
Iceberg snapshot ID, appending only new rows) is a reasonable v2 upgrade
and a good excuse to exercise Iceberg's incremental-read support, but
isn't needed for the MVP.

**Airflow** — built in from the start (not deferred), because the batch
chain (Silver → Gold → ClickHouse sync) is a genuine dependency chain
that benefits from real retries, failure alerting, and backfill support,
and because it's a commonly paired skill with Kafka/Spark in practice.
Webserver + scheduler + shared Postgres metadata DB. DAGs:
- Three collector DAGs (`steam_collector`, `platprices_collector`,
  `rawg_collector`), one per source, each on its own schedule, each
  looping over both regions (IL, US).
- One `itad_backfill` DAG — runs once on initial setup, then re-syncs
  periodically (e.g. monthly) to keep PC-side historical data current.
- One `dim_game` load DAG (loads the seed file into Iceberg; run
  on-demand or whenever the seed file changes).
- One `fx_rates` load DAG — weekly, same cadence as RAWG.
- One batch-chain DAG (`silver_gold_sync`) with Silver → Gold → ClickHouse
  sync as dependent tasks, scheduled daily. Gold applies the `fx_rates`
  table to normalize all prices to USD before computing the Deal Quality
  Score and cross-region/cross-platform comparisons.

**Presentation** — MVP is direct SQL against ClickHouse. No BI tool in
scope; Metabase/Superset is a plausible future add-on.

## Risks / things to validate early, before relying on them architecturally

1. **PlatPrices `/games/batch` on the free tier.** The published docs
   don't explicitly gate this endpoint behind a paid plan (unlike
   price-history, which is explicitly paid-only), but this hasn't been
   confirmed with a live call. If batching isn't available on the free
   tier, or the per-page limit differs from 25, the request budget and
   possibly the 100-game target need to be recalculated. **Do this check
   first**, before building the PlatPrices collector.
2. **PlatPrices region codes for `IL` and `US`.** Needs a live check
   against PlatPrices' `/regions` endpoint before the collector is built
   — Israel is confirmed to be a real PlayStation Store region, but
   whether PlatPrices uses the code `IL` (vs. some other code) for it is
   unconfirmed.
3. **ITAD's actual free-tier rate limits and exact endpoint paths**
   weren't confirmed from official docs — check github.com/isthereanydeal/api
   before building the backfill job.
4. **RAWG → Steam appid linkage.** The design assumes RAWG's `stores`
   field can be parsed to find a game's Steam listing (useful for
   cross-checking the hand-curated mapping), but this hasn't been
   confirmed live. Not load-bearing — the mapping table is hand-curated
   regardless — but worth a quick check.
5. **Steam's unofficial endpoint** could change or disappear without
   notice. The collector should fail gracefully (log and skip, not crash
   the DAG) if its shape changes.

## What's left to the builder

- The remaining ~80 games for the watchlist (20 were already selected
  during brainstorming; the schema and curation process are defined
  here, the specific titles are not).
- Exact Kafka message schemas (JSON payload shape) per source.
- The Deal Quality Score formula — a reasonable first version (weighted
  combination of distance from historical low, typical discount depth,
  and sale frequency) should be proposed during planning and iterated on
  once real data exists, rather than finalized speculatively here.
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
