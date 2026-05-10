# Structured Streaming Project — Kafka → Spark → Neo4j

ITESO · Ingeniería en Sistemas Computacionales · *Procesamiento de Datos Masivos* · Proyecto Final (Parte II — Streaming).

This project takes the synthetic-transaction generator from the batch project (Part I) and turns it into a **real-time pipeline**: a Kafka producer streams transactions continuously, a Spark Structured Streaming consumer parses + filters + windows them, and a Neo4j graph stores both the raw transaction subgraph and a 1-minute windowed `CategoryStats` aggregate. Everything runs locally via Docker Compose, kicked off by a single `python run.py`.

```
┌──────────────┐      JSON       ┌──────────────┐                  ┌────────────────────┐
│              │   transactions  │              │  Structured       │                    │
│   Producer   │ ──────────────▶ │   Kafka      │  Streaming        │       Neo4j        │
│ (kafka-py)   │   topic         │ (1 broker,   │ ─── readStream ──▶│ (Customer,         │
│              │  3 partitions   │  Zookeeper)  │                   │  Merchant,         │
└──────────────┘                 └──────┬───────┘                   │  Transaction,      │
                                        │                           │  CategoryStats)    │
                                        │   Kafka UI :8080          │                    │
                                        ▼                           └────────▲───────────┘
                                  ┌──────────────┐                           │
                                  │   Spark      │   foreachBatch (x2)       │
                                  │ Structured   │ ──────────────────────────┘
                                  │  Streaming   │
                                  └──────────────┘
```

---

## Team

| Nombre | Matrícula |
| --- | --- |
| Francisco Javier De la Torre Silva | 745974 |
| Mauricio Figueroa Guerrero | 749273 |
| Santiago Villa Rodríguez | 744676 |

---

## Prerequisites

- **Docker Desktop** running (Compose v2).
- **`uv`** Python package manager — install with:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- **Java 17+** on the host (Spark 3.5 requires it). Verify with `java -version`.

---

## Quickstart

```bash
uv sync
cp .env.example .env
uv run python run.py
```

`run.py` will:
1. `docker compose up -d` for Kafka + Zookeeper + Kafka-UI + Neo4j.
2. Wait until both Kafka and Neo4j answer health probes.
3. Create the `transactions` topic (3 partitions, RF=1).
4. Seed Neo4j with 1000 `:Customer` + 200 `:Merchant` nodes (idempotent MERGE).
5. Spawn the producer (`src/producer/main.py`).
6. Spawn the Spark consumer (`spark-submit` with pinned `--packages`).
7. Render a live `rich` dashboard with throughput, Kafka high-water mark, Neo4j counts, and uptime.

Stop everything with **Ctrl+C** — `run.py` will confirm, then SIGTERM the children and tear Compose down.

---

## URLs

| Service | URL | What you'll see |
| --- | --- | --- |
| Kafka broker (external) | `localhost:29092` | TCP only — used by the producer/consumer |
| Kafka UI | http://localhost:8080 | Topics, partitions, consumer groups, message inspector |
| Neo4j Browser | http://localhost:7474 | Cypher REPL + graph visualization (login `neo4j` / `streaming123`) |
| Neo4j Bolt | `bolt://localhost:7687` | Driver endpoint (used by Spark connector + bootstrap loader) |
| Spark UI | http://localhost:4040 | Streaming progress, batch timings, query plans |

---

## Validation Cypher (paste into Neo4j Browser)

**1. Sanity check — what labels exist and how many nodes per label?**
```cypher
MATCH (n)
RETURN labels(n)[0] AS label, count(*) AS count
ORDER BY count DESC;
```

**2. Top 10 categories by total amount in the last hour (windowed aggregate sink):**
```cypher
MATCH (s:CategoryStats)
WHERE s.window_start >= datetime() - duration({hours: 1})
RETURN s.category AS category,
       s.country  AS country,
       sum(s.total_amount) AS total_amount,
       sum(s.txn_count)    AS txn_count
ORDER BY total_amount DESC
LIMIT 10;
```

**3. Customer with most approved transactions (raw graph sink):**
```cypher
MATCH (c:Customer)-[:MADE]->(t:Transaction)
WHERE t.status = 'approved'
RETURN c.customer_id   AS customer_id,
       c.customer_name AS name,
       count(t)        AS approved_count
ORDER BY approved_count DESC
LIMIT 1;
```

**4. Sample subgraph (great for slides):**
```cypher
MATCH p = (c:Customer)-[:MADE]->(t:Transaction)-[:AT]->(m:Merchant)
RETURN p
LIMIT 25;
```

---

## How to stop

Press **Ctrl+C** inside `run.py`. It will prompt for confirmation and then SIGTERM both subprocesses before running `docker compose down`. Logs in `logs/` and Spark checkpoints in `checkpoints/` are preserved between runs (delete `checkpoints/` if you want the consumer to restart from `latest` Kafka offsets).

---

## Troubleshooting

- **Port already in use** — the stack binds to `7474`, `7687`, `8080`, `9092`, `29092`, `4040`. If anything else uses these ports, stop it or remap the host side in `docker-compose.yml`.
- **Kafka advertised listeners** — the broker exposes two listeners: `INTERNAL` (Docker network, port 9092) and `EXTERNAL` (host, port 29092). The host-side Python clients must connect to **`localhost:29092`**; anything inside Compose connects to `kafka:9092`.
- **Spark JVM memory** — if `spark-submit` dies on startup, give the JVM more RAM:
  ```bash
  export SPARK_DRIVER_MEMORY=2g
  ```
- **Neo4j connector version mismatch** — `run.py` pins `org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3`. The `_2.12` (Scala) and `_for_spark_3` (Spark major) coordinates **must** match the Spark on your machine. If you're on Spark 3.4.x or below, the connector version needs to change; consult https://neo4j.com/docs/spark/current/.
- **`spark-submit` not on PATH** — install PySpark with `uv sync` and use the one bundled inside `.venv/lib/python3.11/site-packages/pyspark/bin/spark-submit`, or install Spark separately.
- **Producer stuck at "no brokers available"** — the producer retries every 5s. Confirm Kafka is healthy via Kafka UI (http://localhost:8080) or `docker compose logs kafka`.
- **Consumer prints "data loss"** — Spark is past the Kafka retention window. We set `failOnDataLoss=false`; if you want strict mode, delete `checkpoints/` and restart so it resumes from `latest`.
- **No `CategoryStats` nodes appearing** — windowed aggregation needs ~1 minute of data plus the watermark threshold to emit its first result. Wait at least 2 minutes after the producer starts.

---

## Project layout

```
.
├── pyproject.toml         # uv-managed deps (pyspark 3.5.1, kafka-python, neo4j, rich, dotenv)
├── docker-compose.yml     # Zookeeper, Kafka, Kafka UI, Neo4j
├── run.py                 # orchestrator + live dashboard
├── .env.example           # template — copy to .env
├── README.md              # this file
├── .gitignore
├── src/
│   ├── producer/
│   │   ├── main.py        # Kafka producer with backpressure + reconnect
│   │   └── schemas.py     # dataclasses + value pools + generators
│   ├── consumer/
│   │   └── streaming_app.py  # Spark Structured Streaming app
│   └── bootstrap/
│       └── load_reference_data.py  # seeds Customer + Merchant nodes
├── logs/                  # (gitignored) producer.out / consumer.out
└── checkpoints/           # (gitignored) Spark structured-streaming checkpoints
```
