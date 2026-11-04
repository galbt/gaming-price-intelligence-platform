# Local Infrastructure Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the full local docker-compose stack (Postgres, LocalStack, Kafka, Iceberg REST catalog, Spark standalone cluster, ClickHouse, Airflow) with every component proven to talk to its neighbors, so later plans can build real collectors and jobs on top of infrastructure that's already known to work.

**Architecture:** Four incremental layers, each proven before the next is added: (1) the "dumb" storage/messaging primitives (Postgres, LocalStack, Kafka), (2) the lakehouse (Iceberg REST catalog + Spark, proven with a real write/read round trip through LocalStack), (3) the serving warehouse (ClickHouse, proven to read the table Spark just wrote), (4) the orchestrator (Airflow, proven to reach the other services over the compose network).

**Tech Stack:** Docker Compose, PostgreSQL 16, LocalStack, Apache Kafka 3.8 (KRaft mode), Apache Iceberg 1.6 (REST catalog via `tabulario/iceberg-rest`), Apache Spark 3.5 (official `apache/spark` image, standalone cluster), ClickHouse 24.8, Apache Airflow 2.10 (LocalExecutor).

**Spec:** `docs/superpowers/specs/2026-11-03-gaming-price-intelligence-design.md`

## Global Constraints

- Runs fully locally via docker-compose; no cloud dependency of any kind (no AWS, no managed services). [spec: Constraints]
- Kafka runs in KRaft mode — no Zookeeper. [spec: Architecture — Kafka]
- Iceberg catalog is a REST catalog backed by Postgres, not Nessie or a Hive metastore (spec left this as the default; this plan locks it in). [spec: What's left to the builder]
- Spark runs as a standalone cluster (1 master + 2 workers), sized for a laptop: 1-2 cores / ~2GB RAM per worker. [spec: Architecture — Spark]
- LocalStack is the local S3-compatible object store backing the Iceberg warehouse. [spec: Architecture — Iceberg]
- ClickHouse is the serving warehouse and must be able to read Iceberg tables natively (via its built-in Iceberg table function), not just via a separate sync tool. [spec: Architecture — ClickHouse]
- Airflow uses the same Postgres instance as the Iceberg catalog, in a separate database, to limit the number of containers on a laptop. [spec: Architecture — Airflow]
- No service may use hardcoded credentials inside `docker-compose.yml` — all secrets come from `.env` (gitignored), referenced via `${VAR}` substitution.

**Pinned versions** (this plan's own decisions — not fixed by the spec, but later plans must use these same versions for consistency):

| Component | Image / package | Version |
|---|---|---|
| Postgres | `postgres` | `16` |
| LocalStack | `localstack/localstack` | `3.8` |
| AWS CLI (bucket init only) | `amazon/aws-cli` | `2.17.62` |
| Kafka | `apache/kafka` | `3.8.0` |
| Kafka UI | `provectuslabs/kafka-ui` | `v0.7.2` |
| Iceberg REST catalog | `tabulario/iceberg-rest` | `1.6.0` |
| Spark | `apache/spark` | `3.5.3` |
| Iceberg Spark runtime | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` | `1.6.1` |
| Hadoop AWS connector | `org.apache.hadoop:hadoop-aws` | `3.3.4` |
| AWS SDK bundle | `com.amazonaws:aws-java-sdk-bundle` | `1.12.262` |
| Spark-Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12` | `3.5.3` |
| ClickHouse | `clickhouse/clickhouse-server` | `24.8` |
| Airflow | `apache/airflow` | `2.10.2` |

**Known configuration risks** — these are my best-known working configuration for each image, but I can't run them live to verify from here. If a step's exact command/env-var contract doesn't match the installed image's behavior, consult that image's current README/docs for the equivalent, rather than guessing:
- Kafka's exact KRaft env-var names (Task 1).
- LocalStack's exact persistence env-var contract and health-endpoint response shape for the installed version (Task 1) — this plan was corrected mid-execution to replace MinIO (whose free Docker images were discontinued) with LocalStack; see the ledger for that ruling.
- The Iceberg REST catalog image's exact env-var names for its JDBC/S3 config (Task 2).
- Spark was corrected mid-execution too: `bitnami/spark` no longer has current tags on Docker Hub (Bitnami moved free images to an unmaintained `bitnamilegacy` namespace in 2025-2026). Replaced with the official `apache/spark` image, run in standalone mode directly via `spark-class` rather than Bitnami's `SPARK_MODE` env-var convention; see the ledger for that ruling.
- ClickHouse's exact `icebergS3()` table function signature for the installed version (Task 3).

## Review Focus

- **Cold start / restart race conditions**: a dependent service (e.g. Iceberg REST catalog) starting before Postgres or LocalStack is actually ready to accept connections, and failing instead of waiting. Covered by: Task 1's healthchecks + restart-cycle test, reused by every later task's `depends_on`.
- **Idempotent re-runs**: running `docker compose up` a second time against already-initialized state (existing LocalStack bucket, existing Postgres databases) should not error. Covered by: Task 1's restart-cycle test.
- **Committed secrets**: default or hardcoded credentials ending up inside `docker-compose.yml` or git history instead of `.env`. Covered by: Task 1's `.env`/gitignore check.
- **Host port collisions**: two services mapped to the same host port, or a chosen port colliding with something already running on the developer's machine. Covered by: Task 1 documents the intended port map (which later tasks must follow); Task 4's Step 6 runs the actual uniqueness check once the full compose file exists.
- **Cross-component version incompatibility**: the Iceberg REST catalog's spec version, Spark's Iceberg runtime, and ClickHouse's Iceberg reader disagreeing about table format details. Covered by: Task 2's write/read round trip and Task 3's ClickHouse read — if these pass, the three components are provably compatible with each other; no separate test needed.
- **Laptop resource exhaustion**: all eight+ containers running simultaneously exceeding available RAM/CPU, causing silent OOM kills instead of a clear error. Covered by: Task 4's full-stack-up resource check (last task, once everything is running together).

---

## Task 1: Core data services — Postgres, LocalStack, Kafka

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `.gitignore`
- Create: `infra/postgres/init-multiple-dbs.sh`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (for later tasks):
  - Postgres at `postgres:5432` (internal) / `localhost:5433` (host — 5432 was already taken by an unrelated project on this machine), databases `postgres`, `iceberg_catalog`, `airflow`, credentials `${POSTGRES_USER}` / `${POSTGRES_PASSWORD}`.
  - LocalStack S3 API at `localstack:4566` (internal) / `localhost:4566` (host), bucket `warehouse` already created, credentials `${AWS_ACCESS_KEY_ID}` / `${AWS_SECRET_ACCESS_KEY}` / `${AWS_DEFAULT_REGION}`.
  - Kafka broker at `kafka:9092` (internal) / `localhost:9092` (host).
  - Kafka UI at `localhost:8089`.
  - Finalized host port map (see Step 1) that every later task must follow without reusing a port.

- [ ] **Step 1: Write `.gitignore` and `.env.example`**

`.gitignore`:
```
.env
__pycache__/
*.pyc
```

`.env.example` (copy this to `.env` before first run; `.env` itself is gitignored):
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
```

(`test`/`test` is the LocalStack community-edition convention — it doesn't enforce real AWS auth, but Spark's S3A connector and Iceberg's S3 client still require *some* non-empty credential pair to be present.)

Host port map locked in by this plan (record as a comment block at the top of `docker-compose.yml` once it exists):
```
# Host port map — keep every later addition unique against this list:
#   5433  postgres (host 5432 was already taken by an unrelated project)
#   4566  localstack (S3 edge/gateway)
#   9092  kafka
#   8089  kafka-ui
#   8181  iceberg-rest        (Task 2)
#   7077  spark-master (RPC)  (Task 2)
#   8080  spark-master (UI)   (Task 2)
#   8081  spark-worker-1 (UI) (Task 2)
#   8082  spark-worker-2 (UI) (Task 2)
#   8123  clickhouse (HTTP)   (Task 3)
#   9010  clickhouse (native) (Task 3)
#   8090  airflow-webserver   (Task 4)
```

- [ ] **Step 2: Write the Postgres multi-database init script**

`infra/postgres/init-multiple-dbs.sh` (must be executable — `chmod +x` after creating it):
```bash
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE iceberg_catalog;
    CREATE DATABASE airflow;
EOSQL
```

- [ ] **Step 3: Write `docker-compose.yml` with postgres, localstack, localstack-init, kafka, kafka-ui**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./infra/postgres/init-multiple-dbs.sh:/docker-entrypoint-initdb.d/init-multiple-dbs.sh
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      timeout: 5s
      retries: 10

  localstack:
    image: localstack/localstack:3.8
    environment:
      SERVICES: s3
      PERSISTENCE: 1
    volumes:
      - localstack-data:/var/lib/localstack
    ports:
      - "4566:4566"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 5s
      timeout: 5s
      retries: 10

  localstack-init:
    image: amazon/aws-cli:2.17.62
    depends_on:
      localstack:
        condition: service_healthy
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      AWS_REGION: ${AWS_DEFAULT_REGION}
    entrypoint: ["/bin/sh", "-c"]
    command: ["aws --endpoint-url=http://localstack:4566 s3 mb s3://warehouse || true"]

  kafka:
    image: apache/kafka:3.8.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    ports:
      - "9092:9092"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    ports:
      - "8089:8080"

volumes:
  postgres-data:
  localstack-data:
```

- [ ] **Step 4: Bring the stack up and verify Postgres**

Run: `cp .env.example .env && docker compose up -d postgres localstack localstack-init kafka kafka-ui`

Then: `docker compose exec postgres psql -U postgres -l`
Expected: output lists `postgres`, `iceberg_catalog`, and `airflow` as databases.

- [ ] **Step 5: Verify LocalStack bucket creation**

Run: `docker compose logs localstack-init`
Expected: output includes a line confirming the `warehouse` bucket exists (either "created" or already-exists, since `--ignore-existing` was used), and `docker compose ps localstack-init` shows exit code `0`.

- [ ] **Step 6: Verify Kafka produce/consume**

Run:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic smoke-test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
echo "hello-kafka" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --topic smoke-test --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic smoke-test --bootstrap-server localhost:9092 --from-beginning --max-messages 1
```
Expected: the consumer prints `hello-kafka`.

- [ ] **Step 7: Verify Kafka UI**

Run: `curl -s -o /dev/null -w "%{http_code}" http://localhost:8089`
Expected: `200`. (Optionally open `http://localhost:8089` in a browser and confirm the `smoke-test` topic is visible.)

- [ ] **Step 8: Verify no secrets are committed**

Run: `git check-ignore .env && grep -c '\${' docker-compose.yml`
Expected: first command exits `0` (`.env` is ignored); second command's count is greater than `0` and a manual read of `docker-compose.yml` confirms no literal passwords appear outside `${VAR}` references.

- [ ] **Step 9: Verify clean restart (idempotency)**

Run: `docker compose down && docker compose up -d postgres localstack localstack-init kafka kafka-ui`
Expected: all five services reach a running/completed state again with no errors in `docker compose logs`, and repeating Step 4's database check still shows all three databases (Postgres data persisted via the named volume).

- [ ] **Step 10: Commit**

```bash
git add docker-compose.yml .env.example .gitignore infra/postgres/init-multiple-dbs.sh
git commit -m "infra: add Postgres, LocalStack, and Kafka (KRaft) to local stack"
```

---

## Task 2: Iceberg REST catalog + Spark cluster — proven write/read round trip

**Files:**
- Modify: `docker-compose.yml` (add `iceberg-rest`, `spark-master`, `spark-worker-1`, `spark-worker-2`)
- Create: `infra/spark/spark-defaults.conf`
- Create: `infra/spark/smoke_test.py`

**Interfaces:**
- Consumes: Task 1's `postgres:5432` (databases `iceberg_catalog`), `localstack:4566` (bucket `warehouse`), and their credentials.
- Produces (for later tasks and Task 3):
  - Iceberg REST catalog at `iceberg-rest:8181` (internal) / `localhost:8181`.
  - Spark catalog name `lakehouse` (configured in `spark-defaults.conf`), pointing at the REST catalog and `s3a://warehouse/`.
  - Spark master at `spark-master:7077` (internal), UI at `localhost:8080`; workers' UIs at `localhost:8081` / `localhost:8082`.
  - A proven table `lakehouse.smoke.ping`, physically present under `s3://warehouse/smoke/ping/` in LocalStack — this is what Task 3 reads from ClickHouse.

- [ ] **Step 1: Write `infra/spark/spark-defaults.conf`**

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181
spark.sql.catalog.lakehouse.warehouse=s3a://warehouse/
spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint=http://localstack:4566
spark.sql.catalog.lakehouse.s3.path-style-access=true
spark.hadoop.fs.s3a.endpoint=http://localstack:4566
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
```

Note: `fs.s3a.access.key` / `fs.s3a.secret.key` and the Iceberg catalog's AWS credentials are supplied as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) on the Spark containers in Step 3, not hardcoded here — both the S3A connector and Iceberg's `S3FileIO` fall back to the standard AWS env vars automatically.

- [ ] **Step 2: Write `infra/spark/smoke_test.py`**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.smoke.ping (
        id INT, message STRING
    ) USING iceberg
""")
spark.sql("INSERT INTO lakehouse.smoke.ping VALUES (1, 'hello-iceberg')")

result = spark.sql("SELECT * FROM lakehouse.smoke.ping").collect()
assert len(result) == 1 and result[0]["message"] == "hello-iceberg", f"unexpected result: {result}"

print("SMOKE TEST PASSED")
spark.stop()
```

- [ ] **Step 3: Add `iceberg-rest`, `spark-master`, `spark-worker-1`, `spark-worker-2` to `docker-compose.yml`**

```yaml
  iceberg-rest:
    image: tabulario/iceberg-rest:1.6.0
    depends_on:
      postgres:
        condition: service_healthy
      localstack-init:
        condition: service_completed_successfully
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      AWS_REGION: ${AWS_DEFAULT_REGION}
      CATALOG_CATALOG__IMPL: org.apache.iceberg.jdbc.JdbcCatalog
      CATALOG_URI: jdbc:postgresql://postgres:5432/iceberg_catalog
      CATALOG_JDBC_USER: ${POSTGRES_USER}
      CATALOG_JDBC_PASSWORD: ${POSTGRES_PASSWORD}
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://localstack:4566
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
    ports:
      - "8181:8181"
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'echo > /dev/tcp/127.0.0.1/8181'"]
      interval: 5s
      timeout: 5s
      retries: 10

  spark-master:
    image: apache/spark:3.5.3
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "0.0.0.0"]
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      AWS_REGION: ${AWS_DEFAULT_REGION}
      HOME: /tmp
      JAVA_TOOL_OPTIONS: -Duser.home=/tmp
    depends_on:
      iceberg-rest:
        condition: service_healthy
    volumes:
      - ./infra/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./infra/spark:/opt/spark-apps
    ports:
      - "7077:7077"
      - "8080:8080"

  spark-worker-1:
    image: apache/spark:3.5.3
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077", "--cores", "2", "--memory", "2G", "--host", "0.0.0.0"]
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      AWS_REGION: ${AWS_DEFAULT_REGION}
      HOME: /tmp
      JAVA_TOOL_OPTIONS: -Duser.home=/tmp
    depends_on:
      - spark-master
    volumes:
      - ./infra/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
    ports:
      - "8081:8081"

  spark-worker-2:
    image: apache/spark:3.5.3
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077", "--cores", "2", "--memory", "2G", "--host", "0.0.0.0"]
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      AWS_REGION: ${AWS_DEFAULT_REGION}
      HOME: /tmp
      JAVA_TOOL_OPTIONS: -Duser.home=/tmp
    depends_on:
      - spark-master
    volumes:
      - ./infra/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
    ports:
      - "8082:8081"
```

- [ ] **Step 4: Bring up the new services**

Run: `docker compose up -d iceberg-rest spark-master spark-worker-1 spark-worker-2`
Expected: `docker compose ps` shows all four as `Up`; `http://localhost:8080` (Spark master UI) shows 2 workers registered.

- [ ] **Step 5: Run the smoke test**

Run: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/smoke_test.py`
Expected: stdout includes `SMOKE TEST PASSED`. (First run will take longer while `spark.jars.packages` downloads dependencies.)

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml infra/spark/
git commit -m "infra: add Iceberg REST catalog and Spark cluster, proven with write/read smoke test"
```

---

## Task 3: ClickHouse — proven native Iceberg read

**Files:**
- Modify: `docker-compose.yml` (add `clickhouse`)

**Interfaces:**
- Consumes: Task 2's `lakehouse.smoke.ping` table, physically at `s3://warehouse/smoke/ping/` in LocalStack (`localstack:4566`), and Task 1's LocalStack credentials.
- Produces: ClickHouse reachable at `clickhouse:8123` (internal) / `localhost:8123` (HTTP) and `localhost:9010` (native), proven able to read an Iceberg table directly from LocalStack.

- [ ] **Step 1: Add `clickhouse` to `docker-compose.yml`**

```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    depends_on:
      localstack:
        condition: service_healthy
    ports:
      - "8123:8123"
      - "9010:9000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8123/ping"]
      interval: 5s
      timeout: 5s
      retries: 10
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
```

- [ ] **Step 2: Bring up ClickHouse**

Run: `docker compose up -d clickhouse`
Expected: `docker compose ps clickhouse` shows `Up (healthy)`.

- [ ] **Step 3: Query the Iceberg table natively from ClickHouse**

Run:
```bash
docker compose exec clickhouse clickhouse-client --query "
  SELECT * FROM icebergS3(
    'http://localstack:4566/warehouse/smoke/ping/',
    'test', 'test'
  )
"
```
(Credentials are the literal `.env` values from Step 1 of Task 1, inlined here because this command runs in the host shell, which doesn't read `.env` the way `docker compose` does.)

Expected: output is one row, `1\thello-iceberg`. If this exact function call errors on the installed ClickHouse version, check `clickhouse-client --query "SELECT * FROM system.functions WHERE name ILIKE '%iceberg%'"` for the available function/engine name on this version and adjust the call accordingly — this is the version-sensitivity risk flagged in Global Constraints.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "infra: add ClickHouse, proven to read Iceberg tables natively from LocalStack"
```

---

## Task 4: Airflow orchestration — proven reachability + full-stack resource check

**Files:**
- Modify: `docker-compose.yml` (add `airflow-init`, `airflow-webserver`, `airflow-scheduler`)
- Create: `infra/airflow/dags/smoke_test_dag.py`

**Interfaces:**
- Consumes: Task 1's `postgres:5432` (database `airflow`), Task 2's `iceberg-rest:8181`.
- Produces: Airflow webserver at `localhost:8090` (login `${AIRFLOW_ADMIN_USER}` / `${AIRFLOW_ADMIN_PASSWORD}`), scheduler running, DAG `smoke_test` proven to execute successfully end to end. Later plans' collector DAGs and the batch-chain DAG are added as sibling files under `infra/airflow/dags/`.

- [ ] **Step 1: Add Airflow credentials and Fernet key to `.env.example`**

Append to `.env.example`:
```
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_FERNET_KEY=
```
Generate a real key for your own `.env` (not committed) with:
`python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
and paste it as the value of `AIRFLOW_FERNET_KEY` in `.env`.

- [ ] **Step 2: Write `infra/airflow/dags/smoke_test_dag.py`**

```python
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_iceberg_catalog_reachable():
    response = requests.get("http://iceberg-rest:8181/v1/config", timeout=5)
    response.raise_for_status()


with DAG(
    dag_id="smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="check_iceberg_catalog_reachable",
        python_callable=check_iceberg_catalog_reachable,
    )
```

- [ ] **Step 3: Add `airflow-init`, `airflow-webserver`, `airflow-scheduler` to `docker-compose.yml`**

```yaml
  airflow-init:
    image: apache/airflow:2.10.2
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    entrypoint: /bin/bash
    command: >
      -c "airflow db migrate &&
      airflow users create --username ${AIRFLOW_ADMIN_USER} --password ${AIRFLOW_ADMIN_PASSWORD} --firstname admin --lastname admin --role Admin --email admin@example.com"

  airflow-webserver:
    image: apache/airflow:2.10.2
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    volumes:
      - ./infra/airflow/dags:/opt/airflow/dags
    ports:
      - "8090:8080"
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.10.2
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    volumes:
      - ./infra/airflow/dags:/opt/airflow/dags
    command: scheduler
```

- [ ] **Step 4: Bring up Airflow**

Run: `docker compose up -d airflow-init airflow-webserver airflow-scheduler`
Expected: `docker compose logs airflow-init` shows the admin user created with no errors; `docker compose ps` shows `airflow-webserver` and `airflow-scheduler` as `Up`.

- [ ] **Step 5: Run the smoke test DAG**

Run: `docker compose exec airflow-webserver airflow dags test smoke_test $(date +%F)`
Expected: output shows the `check_iceberg_catalog_reachable` task succeeding and the DAG run finishing with no exceptions. (Log in at `http://localhost:8090` with `${AIRFLOW_ADMIN_USER}`/`${AIRFLOW_ADMIN_PASSWORD}` to see it visually, optional.)

- [ ] **Step 6: Full-stack resource check and port-uniqueness check**

Run: `grep -oE '"[0-9]+:[0-9]+"' docker-compose.yml | cut -d: -f1 | tr -d '"' | sort | uniq -d`
Expected: no output (an empty result means every host port in the finished compose file, across all four tasks, is unique — any line printed is a duplicate host port that must be fixed before continuing).

Run: `docker compose up -d` (brings up every remaining service) followed by `docker compose ps` and `docker stats --no-stream`
Expected: every service in `docker compose ps` shows `Up` (none `Restarting` or `Exited` from an OOM kill); `docker stats` shows no container pinned at its memory limit. If a container is being killed, lower `SPARK_WORKER_MEMORY` (Task 2) or stop non-essential services before running the full stack, and note the adjustment here.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml .env.example infra/airflow/
git commit -m "infra: add Airflow orchestration, proven reachable to the lakehouse"
```
