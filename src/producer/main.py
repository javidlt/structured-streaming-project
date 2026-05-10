"""Kafka producer streaming synthetic financial transactions.

Features:
- JSON serialization, configurable inter-message delay.
- Backpressure handling via send().get(timeout=...) + retry loop.
- Automatic reconnect on broker outage (NoBrokersAvailable / KafkaTimeoutError).
- Clean shutdown on SIGTERM / SIGINT.
"""
from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.producer.schemas import generate_transaction  # noqa: E402

logger = logging.getLogger("producer")

_RUNNING = True


def _install_signal_handlers() -> None:
    def _stop(signum: int, _frame: object) -> None:
        global _RUNNING
        logger.info("received signal %s, draining and shutting down", signum)
        _RUNNING = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | producer | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Build a KafkaProducer with retry on broker unavailable."""
    attempt = 0
    while _RUNNING:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                acks="all",
                retries=5,
                linger_ms=20,
                max_in_flight_requests_per_connection=5,
                request_timeout_ms=30_000,
                reconnect_backoff_ms=500,
                reconnect_backoff_max_ms=10_000,
            )
            logger.info("connected to Kafka at %s (attempt %d)", bootstrap_servers, attempt)
            return producer
        except NoBrokersAvailable:
            logger.warning("no brokers available, retry in 5s (attempt %d)", attempt)
            time.sleep(5)
    raise RuntimeError("producer shutdown requested before broker became available")


def run() -> int:
    _configure_logging()
    _install_signal_handlers()
    load_dotenv()

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_TOPIC"]
    interval_ms = int(os.environ.get("PRODUCER_INTERVAL_MS", "200"))
    log_every = int(os.environ.get("PRODUCER_LOG_EVERY", "50"))
    num_customers = int(os.environ.get("PRODUCER_NUM_CUSTOMERS", "1000"))
    num_merchants = int(os.environ.get("PRODUCER_NUM_MERCHANTS", "200"))

    logger.info(
        "starting producer topic=%s interval=%dms customers=%d merchants=%d",
        topic, interval_ms, num_customers, num_merchants,
    )

    producer = _build_producer(bootstrap)
    rng = random.Random()
    txn_id = 0
    sent = 0
    errors = 0

    try:
        while _RUNNING:
            txn_id += 1
            txn = generate_transaction(txn_id, num_customers, num_merchants, rng)
            payload = txn.to_dict()

            try:
                future = producer.send(topic, key=txn.transaction_id, value=payload)
                # block briefly to surface backpressure; soaks linger_ms in normal path
                future.get(timeout=10)
                sent += 1
                if sent % log_every == 0:
                    logger.info("produced %d transactions (last id=%d)", sent, txn_id)
            except KafkaTimeoutError:
                errors += 1
                logger.warning("send timeout for txn=%d (errors=%d)", txn_id, errors)
            except KafkaError as exc:
                errors += 1
                logger.warning("kafka error txn=%d err=%s (errors=%d)", txn_id, exc, errors)
                # rebuild producer on persistent failure
                if errors % 10 == 0:
                    logger.warning("rebuilding producer after repeated errors")
                    try:
                        producer.close(timeout=5)
                    except Exception:  # noqa: BLE001
                        pass
                    producer = _build_producer(bootstrap)

            time.sleep(interval_ms / 1000.0)
    finally:
        logger.info("flushing producer (sent=%d, errors=%d)", sent, errors)
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        except Exception:  # noqa: BLE001
            logger.exception("error while closing producer")
        logger.info("producer stopped cleanly")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
