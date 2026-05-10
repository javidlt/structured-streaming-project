"""Single-entrypoint orchestrator for the streaming pipeline.

Run with::

    uv run python run.py

It will:
  1. Bring up Docker Compose (Kafka + Zookeeper + Kafka-UI + Neo4j).
  2. Wait until Kafka and Neo4j are healthy.
  3. Create the `transactions` topic if it doesn't exist.
  4. Seed Neo4j with reference Customer / Merchant nodes.
  5. Spawn the Kafka producer subprocess.
  6. Spawn the Spark Structured Streaming consumer subprocess.
  7. Render a live `rich` dashboard with throughput / lag / Neo4j counts.
  8. On Ctrl+C, ask for confirmation, then SIGTERM children and `docker compose down`.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError
from kafka.structs import TopicPartition
from neo4j import GraphDatabase
from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

PROJECT_ROOT = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_ROOT / "logs"
CHECKPOINTS_DIR = PROJECT_ROOT / "checkpoints"

# Pinned Spark packages — both must match Spark 3.5.x / Scala 2.12.
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3",
])

console = Console()


# ---------------------------------------------------------------------------
# State tracked across the live dashboard
# ---------------------------------------------------------------------------
@dataclass
class PipelineState:
    started_at: datetime = field(default_factory=datetime.now)
    producer_proc: Optional[subprocess.Popen] = None
    consumer_proc: Optional[subprocess.Popen] = None
    producer_log: Optional[Path] = None
    consumer_log: Optional[Path] = None
    producer_log_handle: Optional[object] = None
    consumer_log_handle: Optional[object] = None
    last_produced: int = 0
    kafka_high_water: int = 0
    consumer_lag: int = 0
    neo4j_counts: dict[str, int] = field(default_factory=dict)
    neo4j_rels: int = 0


# ---------------------------------------------------------------------------
# --log mode tailers
# ---------------------------------------------------------------------------
class KafkaTail:
    """Background thread that reads the topic into a rolling buffer for display.

    Uses no consumer group (group_id=None) so it doesn't interfere with Spark's
    offset tracking. Starts from `latest`, so we only show messages emitted
    after the dashboard came up.
    """

    def __init__(self, bootstrap: str, topic: str, maxlen: int = 15):
        self.bootstrap = bootstrap
        self.topic = topic
        self.messages: collections.deque[tuple[datetime, dict]] = collections.deque(maxlen=maxlen)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="kafka-tail", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap,
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    group_id=None,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=500,
                )
                backoff = 1.0
            except Exception:  # noqa: BLE001
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
                continue

            try:
                while not self._stop.is_set():
                    for record in consumer:
                        if self._stop.is_set():
                            break
                        try:
                            self.messages.append((datetime.now(), record.value))
                        except Exception:  # noqa: BLE001
                            pass
            except Exception:  # noqa: BLE001
                pass
            finally:
                try:
                    consumer.close(autocommit=False)
                except Exception:  # noqa: BLE001
                    pass


_BATCH_LOG_RE = re.compile(r"\[(?P<sink>[\w_]+)\]\s+batch=(?P<batch>\d+)\s+rows=(?P<rows>\d+)")


class ConsumerLogTail:
    """Background thread that scrapes `logs/consumer.out` for foreachBatch lines.

    The Spark consumer logs ``[raw_graph] batch=N rows=M`` and
    ``[category_stats] batch=N rows=M`` for every micro-batch it commits to
    Neo4j. We tail those into a rolling buffer to visualize what the consumer
    is doing on the receiving side of the pipeline.
    """

    def __init__(self, path: Path, maxlen: int = 12):
        self.path = path
        self.events: collections.deque[tuple[datetime, str, int, int]] = collections.deque(maxlen=maxlen)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="consumer-log-tail", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        # Wait for the consumer log to appear.
        while not self._stop.is_set() and not self.path.exists():
            time.sleep(0.5)
        if self._stop.is_set():
            return
        try:
            with self.path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(0, os.SEEK_END)
                while not self._stop.is_set():
                    line = fh.readline()
                    if not line:
                        time.sleep(0.5)
                        continue
                    match = _BATCH_LOG_RE.search(line)
                    if match:
                        self.events.append((
                            datetime.now(),
                            match.group("sink"),
                            int(match.group("batch")),
                            int(match.group("rows")),
                        ))
        except Exception:  # noqa: BLE001
            pass


def _render_producer_messages_panel(tail: KafkaTail) -> Panel:
    table = Table(show_header=True, header_style="bold magenta", expand=True, box=None)
    table.add_column("seen_at", style="dim", no_wrap=True)
    table.add_column("txn_id", style="cyan", justify="right")
    table.add_column("cust", justify="right")
    table.add_column("merch", justify="right")
    table.add_column("amount", justify="right")
    table.add_column("cur")
    table.add_column("status")
    table.add_column("country", style="dim")
    table.add_column("payment")
    for seen_at, msg in list(tail.messages):
        status = str(msg.get("status", ""))
        if status == "approved":
            status_styled = "[green]approved[/green]"
        elif status == "declined":
            status_styled = "[red]declined[/red]"
        else:
            status_styled = f"[yellow]{status}[/yellow]"
        try:
            amount = f"{float(msg.get('amount', 0)):,.2f}"
        except (TypeError, ValueError):
            amount = str(msg.get("amount", ""))
        table.add_row(
            seen_at.strftime("%H:%M:%S"),
            str(msg.get("transaction_id", "")),
            str(msg.get("customer_id", "")),
            str(msg.get("merchant_id", "")),
            amount,
            str(msg.get("currency", "")),
            status_styled,
            str(msg.get("transaction_country", "")),
            str(msg.get("payment_method", "")),
        )
    return Panel(
        table,
        title="[bold]producer → kafka — last 15 messages on `transactions`[/bold]",
        border_style="magenta",
        padding=(0, 1),
    )


def _render_consumer_commits_panel(tail: ConsumerLogTail) -> Panel:
    table = Table(show_header=True, header_style="bold blue", expand=True, box=None)
    table.add_column("seen_at", style="dim", no_wrap=True)
    table.add_column("sink", style="cyan")
    table.add_column("batch", justify="right")
    table.add_column("rows committed", justify="right")
    for seen_at, sink, batch, rows in list(tail.events):
        sink_styled = (
            f"[green]{sink}[/green]" if sink == "raw_graph"
            else f"[yellow]{sink}[/yellow]"
        )
        rows_styled = str(rows) if rows > 0 else f"[dim]{rows}[/dim]"
        table.add_row(seen_at.strftime("%H:%M:%S"), sink_styled, str(batch), rows_styled)
    return Panel(
        table,
        title="[bold]spark consumer → neo4j — last 12 foreachBatch commits[/bold]",
        border_style="blue",
        padding=(0, 1),
    )


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------
def _wait_tcp(host: str, port: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(2)
    return False


def _wait_kafka(bootstrap: str, timeout: int) -> bool:
    host, _, port_s = bootstrap.partition(":")
    port = int(port_s or 9092)
    if not _wait_tcp(host, port, timeout):
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
            admin.list_topics()
            admin.close()
            return True
        except (KafkaError, NoBrokersAvailable):
            time.sleep(3)
    return False


def _wait_neo4j(uri: str, user: str, password: str, timeout: int) -> bool:
    deadline = time.time() + timeout
    last_err: Optional[Exception] = None
    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            driver.close()
            return True
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(3)
    if last_err is not None:
        console.print(f"[red]neo4j wait failed: {last_err}[/red]")
    return False


# ---------------------------------------------------------------------------
# Docker + topic + bootstrap
# ---------------------------------------------------------------------------
def _ensure_java() -> None:
    """Spark needs a JRE on the host. Fail fast with a clear message if absent."""
    java = shutil.which("java")
    if java is None:
        console.print(
            "[red]✗ java not found on PATH — Spark cannot run.[/red]\n"
            "  Install OpenJDK 17 with:\n"
            "    [bold]brew install openjdk@17[/bold]\n"
            "    [bold]sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk "
            "/Library/Java/JavaVirtualMachines/openjdk-17.jdk[/bold]"
        )
        sys.exit(1)
    try:
        result = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=10,
        )
        # `java -version` prints to stderr historically.
        version_str = (result.stderr or result.stdout).strip().splitlines()[0]
        console.print(f"  [green]✓ java available[/green] — {version_str}")
    except Exception as exc:  # noqa: BLE001
        console.print(f"[yellow]could not verify java version: {exc}[/yellow]")


def _ensure_docker_compose() -> str:
    if shutil.which("docker") is None:
        console.print("[red]docker not found in PATH[/red]")
        sys.exit(1)
    # Try the v2 plugin first.
    result = subprocess.run(["docker", "compose", "version"], capture_output=True)
    if result.returncode == 0:
        return "docker compose"
    if shutil.which("docker-compose"):
        return "docker-compose"
    console.print("[red]neither `docker compose` nor `docker-compose` is available[/red]")
    sys.exit(1)


def _docker_up(compose_cmd: str) -> None:
    console.print(f"[bold cyan]▶ {compose_cmd} up -d[/bold cyan]")
    proc = subprocess.Popen(
        compose_cmd.split() + ["up", "-d"],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        console.print(f"  [grey50]{line.rstrip()}[/grey50]")
    proc.wait()
    if proc.returncode != 0:
        console.print("[red]docker compose up failed[/red]")
        sys.exit(proc.returncode)


def _docker_down(compose_cmd: str) -> None:
    console.print(f"[bold cyan]▶ {compose_cmd} down[/bold cyan]")
    subprocess.run(compose_cmd.split() + ["down"], cwd=PROJECT_ROOT)


def _ensure_topic(bootstrap: str, topic: str, partitions: int, rf: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10_000)
    try:
        existing = admin.list_topics()
        if topic in existing:
            console.print(f"  topic [green]{topic}[/green] already exists")
            return
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions,
                                      replication_factor=rf)])
        console.print(f"  topic [green]{topic}[/green] created "
                      f"(partitions={partitions}, rf={rf})")
    except TopicAlreadyExistsError:
        console.print(f"  topic [green]{topic}[/green] already exists")
    finally:
        admin.close()


def _run_bootstrap_loader() -> None:
    console.print("[bold cyan]▶ seeding Neo4j with reference data[/bold cyan]")
    env = os.environ.copy()
    proc = subprocess.run(
        [sys.executable, "-m", "src.bootstrap.load_reference_data"],
        cwd=PROJECT_ROOT,
        env=env,
    )
    if proc.returncode != 0:
        console.print("[red]bootstrap loader failed[/red]")
        sys.exit(proc.returncode)


# ---------------------------------------------------------------------------
# Subprocess launchers
# ---------------------------------------------------------------------------
def _spawn_producer(state: PipelineState) -> None:
    log_path = LOGS_DIR / "producer.out"
    state.producer_log = log_path
    handle = log_path.open("ab", buffering=0)
    state.producer_log_handle = handle
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    state.producer_proc = subprocess.Popen(
        [sys.executable, "-m", "src.producer.main"],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    console.print(f"  producer pid=[bold]{state.producer_proc.pid}[/bold] → {log_path}")


def _spawn_consumer(state: PipelineState) -> None:
    log_path = LOGS_DIR / "consumer.out"
    state.consumer_log = log_path
    handle = log_path.open("ab", buffering=0)
    state.consumer_log_handle = handle
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    spark_submit = shutil.which("spark-submit")
    if spark_submit is None:
        # PySpark ships spark-submit inside its package; fall back to `python -m`.
        cmd = [
            sys.executable, "-m", "pyspark",
            "--packages", SPARK_PACKAGES,
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            str(PROJECT_ROOT / "src" / "consumer" / "streaming_app.py"),
        ]
        # `python -m pyspark` doesn't accept a script; use the actual entrypoint.
        cmd = [
            sys.executable, "-c",
            "import os, sys; from pyspark.context import SparkContext; "
            "from pyspark.find_spark_home import _find_spark_home; "
            "home=_find_spark_home(); "
            "os.execv(os.path.join(home,'bin','spark-submit'), "
            "['spark-submit','--packages', "
            f"'{SPARK_PACKAGES}','--conf','spark.jars.ivy=/tmp/.ivy2', "
            f"'{PROJECT_ROOT / 'src' / 'consumer' / 'streaming_app.py'}'])",
        ]
    else:
        cmd = [
            spark_submit,
            "--packages", SPARK_PACKAGES,
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            str(PROJECT_ROOT / "src" / "consumer" / "streaming_app.py"),
        ]

    state.consumer_proc = subprocess.Popen(
        cmd, cwd=PROJECT_ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT,
    )
    console.print(f"  consumer pid=[bold]{state.consumer_proc.pid}[/bold] → {log_path}")


# ---------------------------------------------------------------------------
# Metrics scrapers (best-effort, never raise)
# ---------------------------------------------------------------------------
_PRODUCED_RE = re.compile(r"produced\s+(\d+)\s+transactions")


def _scrape_producer_count(log_path: Optional[Path]) -> int:
    if log_path is None or not log_path.exists():
        return 0
    try:
        # Read last ~8 KiB and find the highest number we logged.
        with log_path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - 8192))
            tail = fh.read().decode("utf-8", errors="replace")
        matches = _PRODUCED_RE.findall(tail)
        if not matches:
            return 0
        return max(int(m) for m in matches)
    except OSError:
        return 0


def _scrape_kafka_metrics(bootstrap: str, topic: str) -> tuple[int, int]:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            enable_auto_commit=False,
            consumer_timeout_ms=2000,
        )
        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in partitions]
        if not tps:
            consumer.close()
            return 0, 0
        end_offsets = consumer.end_offsets(tps)
        # We don't track a consumer group from Spark (it uses its own checkpoints),
        # so report high-water-mark only; lag stays 0.
        hwm = sum(end_offsets.values())
        consumer.close()
        return hwm, 0
    except Exception:  # noqa: BLE001
        return 0, 0


def _scrape_neo4j(uri: str, user: str, password: str) -> tuple[dict[str, int], int]:
    counts: dict[str, int] = {"Customer": 0, "Merchant": 0,
                              "Transaction": 0, "CategoryStats": 0}
    rels = 0
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            for label in counts:
                res = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()
                if res is not None:
                    counts[label] = int(res["c"])
            rel_res = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()
            if rel_res is not None:
                rels = int(rel_res["c"])
        driver.close()
    except Exception:  # noqa: BLE001
        pass
    return counts, rels


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _render_ready_panel() -> Panel:
    body = Text.from_markup(
        "[bold]Kafka broker[/bold]        localhost:29092\n"
        "[bold]Kafka UI[/bold]            http://localhost:8080\n"
        "[bold]Neo4j Browser[/bold]       http://localhost:7474\n"
        "                    (neo4j / streaming123)\n"
        "[bold]Neo4j Bolt[/bold]          bolt://localhost:7687\n"
        "[bold]Spark UI[/bold]            http://localhost:4040\n"
        "\n"
        "[bold]Producer log[/bold]        ./logs/producer.out\n"
        "[bold]Consumer log[/bold]        ./logs/consumer.out\n"
        "[bold]Live tail[/bold]           tail -f logs/*.out"
    )
    return Panel(
        Align.left(body),
        title="[bold green]STREAMING PIPELINE READY[/bold green]",
        border_style="green",
        padding=(1, 2),
    )


def _format_uptime(started: datetime) -> str:
    delta: timedelta = datetime.now() - started
    secs = int(delta.total_seconds())
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _render_dashboard(state: PipelineState, topic: str) -> Group:
    table = Table(show_header=True, header_style="bold magenta", expand=True)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    producer_status = "running" if state.producer_proc and state.producer_proc.poll() is None else "stopped"
    consumer_status = "running" if state.consumer_proc and state.consumer_proc.poll() is None else "stopped"

    table.add_row("uptime", _format_uptime(state.started_at))
    table.add_row("producer", f"[{'green' if producer_status == 'running' else 'red'}]{producer_status}[/]")
    table.add_row("consumer", f"[{'green' if consumer_status == 'running' else 'red'}]{consumer_status}[/]")
    table.add_row("transactions produced", str(state.last_produced))
    table.add_row(f"kafka high-water ({topic})", str(state.kafka_high_water))
    table.add_row("consumer lag (group-less, Spark checkpoints)", str(state.consumer_lag))
    table.add_row("Neo4j :Customer", str(state.neo4j_counts.get("Customer", 0)))
    table.add_row("Neo4j :Merchant", str(state.neo4j_counts.get("Merchant", 0)))
    table.add_row("Neo4j :Transaction", str(state.neo4j_counts.get("Transaction", 0)))
    table.add_row("Neo4j :CategoryStats", str(state.neo4j_counts.get("CategoryStats", 0)))
    table.add_row("Neo4j relationships", str(state.neo4j_rels))

    return Group(
        _render_ready_panel(),
        Panel(table, title="[bold]live metrics[/bold] (Ctrl+C to stop)",
              border_style="cyan", padding=(1, 2)),
    )


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------
def _stop_child(proc: Optional[subprocess.Popen], name: str, grace: float = 8.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    console.print(f"  stopping {name} (pid={proc.pid})")
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        console.print(f"  [yellow]{name} did not exit after SIGTERM, killing[/yellow]")
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            console.print(f"  [red]{name} survived SIGKILL?[/red]")
    except Exception as exc:  # noqa: BLE001
        console.print(f"  [red]error stopping {name}: {exc}[/red]")


def _shutdown(state: PipelineState, compose_cmd: str) -> None:
    _stop_child(state.producer_proc, "producer")
    _stop_child(state.consumer_proc, "consumer", grace=15.0)
    for handle in (state.producer_log_handle, state.consumer_log_handle):
        try:
            if handle is not None:
                handle.close()
        except Exception:  # noqa: BLE001
            pass
    _docker_down(compose_cmd)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Bring up the Kafka → Spark → Neo4j streaming pipeline.",
    )
    parser.add_argument(
        "--log",
        action="store_true",
        help=(
            "Show two extra live panels: every producer message landing on the "
            "Kafka topic (last 15) and every foreachBatch commit the Spark "
            "consumer writes to Neo4j (last 12)."
        ),
    )
    return parser.parse_args(argv)


def main() -> int:
    args = _parse_args()
    load_dotenv()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
    # Clear previous logs so the dashboard starts from a clean count.
    for f in LOGS_DIR.glob("*.out"):
        try:
            f.unlink()
        except OSError:
            pass

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.environ.get("KAFKA_TOPIC", "transactions")
    partitions = int(os.environ.get("KAFKA_NUM_PARTITIONS", "3"))
    rf = int(os.environ.get("KAFKA_REPLICATION_FACTOR", "1"))
    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.environ.get("NEO4J_USER", "neo4j")
    neo4j_pwd = os.environ.get("NEO4J_PASSWORD", "streaming123")
    refresh = int(os.environ.get("DASHBOARD_REFRESH_SECONDS", "3"))
    health_timeout = int(os.environ.get("HEALTHCHECK_TIMEOUT_SECONDS", "180"))

    _ensure_java()
    compose_cmd = _ensure_docker_compose()
    state = PipelineState()

    # Install our own signal handlers so a stray Ctrl+C doesn't leak children.
    interrupted = {"flag": False}

    def _sigint(_signum: int, _frame: object) -> None:
        interrupted["flag"] = True

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    try:
        _docker_up(compose_cmd)

        console.print("[bold cyan]▶ waiting for Kafka to become healthy[/bold cyan]")
        if not _wait_kafka(bootstrap, timeout=health_timeout):
            console.print("[red]Kafka did not become healthy in time[/red]")
            _docker_down(compose_cmd)
            return 1
        console.print("  [green]✓ kafka is up[/green]")

        console.print("[bold cyan]▶ waiting for Neo4j to become healthy[/bold cyan]")
        if not _wait_neo4j(neo4j_uri, neo4j_user, neo4j_pwd, timeout=health_timeout):
            console.print("[red]Neo4j did not become healthy in time[/red]")
            _docker_down(compose_cmd)
            return 1
        console.print("  [green]✓ neo4j is up[/green]")

        console.print(f"[bold cyan]▶ ensuring topic '{topic}'[/bold cyan]")
        _ensure_topic(bootstrap, topic, partitions, rf)

        _run_bootstrap_loader()

        console.print("[bold cyan]▶ launching producer[/bold cyan]")
        _spawn_producer(state)

        console.print("[bold cyan]▶ launching Spark consumer[/bold cyan]")
        _spawn_consumer(state)

        kafka_tail: Optional[KafkaTail] = None
        consumer_log_tail: Optional[ConsumerLogTail] = None
        if args.log:
            console.print("[bold cyan]▶ --log mode: starting message tailers[/bold cyan]")
            kafka_tail = KafkaTail(bootstrap, topic, maxlen=15)
            kafka_tail.start()
            consumer_log_tail = ConsumerLogTail(LOGS_DIR / "consumer.out", maxlen=12)
            consumer_log_tail.start()

        def _build_view() -> Group:
            dashboard = _render_dashboard(state, topic)
            if not args.log:
                return dashboard
            assert kafka_tail is not None and consumer_log_tail is not None
            return Group(
                dashboard,
                _render_producer_messages_panel(kafka_tail),
                _render_consumer_commits_panel(consumer_log_tail),
            )

        # Live dashboard.
        with Live(_build_view(), console=console,
                  refresh_per_second=2, screen=False) as live:
            while not interrupted["flag"]:
                state.last_produced = _scrape_producer_count(state.producer_log)
                state.kafka_high_water, state.consumer_lag = _scrape_kafka_metrics(bootstrap, topic)
                state.neo4j_counts, state.neo4j_rels = _scrape_neo4j(
                    neo4j_uri, neo4j_user, neo4j_pwd
                )
                live.update(_build_view())
                # Exit fast if a child crashed.
                for proc, name in ((state.producer_proc, "producer"),
                                   (state.consumer_proc, "consumer")):
                    if proc is not None and proc.poll() is not None:
                        console.print(f"[yellow]child '{name}' exited (rc={proc.returncode})[/yellow]")
                for _ in range(refresh * 10):
                    if interrupted["flag"]:
                        break
                    time.sleep(0.1)

        if kafka_tail is not None:
            kafka_tail.stop()
        if consumer_log_tail is not None:
            consumer_log_tail.stop()

        console.print()
        try:
            answer = console.input("[bold yellow]Ctrl+C received. Stop services and shutdown? [Y/n] [/bold yellow]")
        except (EOFError, KeyboardInterrupt):
            answer = "y"
        if answer.strip().lower() in ("", "y", "yes"):
            _shutdown(state, compose_cmd)
        else:
            console.print("[yellow]leaving containers and children running. "
                          "Re-run `docker compose down` manually when finished.[/yellow]")
            return 0
        return 0

    except Exception:
        console.print_exception()
        _shutdown(state, compose_cmd)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
