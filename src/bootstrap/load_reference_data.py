"""Bootstrap loader: seeds Neo4j with 1000 customers + 200 merchants via MERGE.

Idempotent — safe to rerun. Also installs uniqueness constraints so the
streaming consumer's MERGE-on-id pattern is efficient.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase

# Allow `python -m src.bootstrap.load_reference_data` and direct invocation.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.producer.schemas import build_customers, build_merchants  # noqa: E402

logger = logging.getLogger("bootstrap")


CONSTRAINTS: tuple[str, ...] = (
    "CREATE CONSTRAINT customer_id_unique IF NOT EXISTS "
    "FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE",
    "CREATE CONSTRAINT merchant_id_unique IF NOT EXISTS "
    "FOR (m:Merchant) REQUIRE m.merchant_id IS UNIQUE",
    "CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS "
    "FOR (t:Transaction) REQUIRE t.transaction_id IS UNIQUE",
    "CREATE CONSTRAINT category_stats_window IF NOT EXISTS "
    "FOR (s:CategoryStats) REQUIRE (s.category, s.country, s.window_start) IS UNIQUE",
)

CUSTOMER_MERGE = """
UNWIND $rows AS row
MERGE (c:Customer {customer_id: row.customer_id})
SET c.customer_name = row.customer_name,
    c.email = row.email,
    c.customer_country = row.customer_country,
    c.customer_city = row.customer_city,
    c.age = row.age,
    c.registration_date = date(row.registration_date)
"""

MERCHANT_MERGE = """
UNWIND $rows AS row
MERGE (m:Merchant {merchant_id: row.merchant_id})
SET m.merchant_name = row.merchant_name,
    m.category = row.category,
    m.merchant_country = row.merchant_country,
    m.merchant_city = row.merchant_city
"""


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | bootstrap | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _install_constraints(driver: Driver, database: str) -> None:
    with driver.session(database=database) as session:
        for stmt in CONSTRAINTS:
            session.run(stmt)
    logger.info("installed %d uniqueness constraints", len(CONSTRAINTS))


def _batch_merge(driver: Driver, database: str, cypher: str, rows: list[dict],
                 label: str, batch_size: int = 500) -> None:
    total = len(rows)
    with driver.session(database=database) as session:
        for offset in range(0, total, batch_size):
            chunk = rows[offset: offset + batch_size]
            session.run(cypher, rows=chunk)
            logger.info("[%s] upserted %d / %d", label, min(offset + batch_size, total), total)


def load_reference_data(num_customers: int, num_merchants: int) -> None:
    uri = os.environ["NEO4J_URI"]
    user = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]
    database = os.environ.get("NEO4J_DATABASE", "neo4j")

    logger.info("connecting to Neo4j at %s (db=%s)", uri, database)
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        driver.verify_connectivity()
        _install_constraints(driver, database)

        customers = [c.to_dict() for c in build_customers(num_customers)]
        merchants = [m.to_dict() for m in build_merchants(num_merchants)]

        logger.info("seeding %d customers", len(customers))
        _batch_merge(driver, database, CUSTOMER_MERGE, customers, label="Customer")

        logger.info("seeding %d merchants", len(merchants))
        _batch_merge(driver, database, MERCHANT_MERGE, merchants, label="Merchant")

        logger.info("bootstrap complete")
    finally:
        driver.close()


def main() -> int:
    _configure_logging()
    load_dotenv()
    num_customers = int(os.environ.get("PRODUCER_NUM_CUSTOMERS", "1000"))
    num_merchants = int(os.environ.get("PRODUCER_NUM_MERCHANTS", "200"))
    try:
        load_reference_data(num_customers, num_merchants)
    except Exception:
        logger.exception("bootstrap failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
