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

import os
import re
import shutil
import signal
import socket
import subprocess
import sys
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
def main() -> int:
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

        # Live dashboard.
        with Live(_render_dashboard(state, topic), console=console,
                  refresh_per_second=2, screen=False) as live:
            while not interrupted["flag"]:
                state.last_produced = _scrape_producer_count(state.producer_log)
                state.kafka_high_water, state.consumer_lag = _scrape_kafka_metrics(bootstrap, topic)
                state.neo4j_counts, state.neo4j_rels = _scrape_neo4j(
                    neo4j_uri, neo4j_user, neo4j_pwd
                )
                live.update(_render_dashboard(state, topic))
                # Exit fast if a child crashed.
                for proc, name in ((state.producer_proc, "producer"),
                                   (state.consumer_proc, "consumer")):
                    if proc is not None and proc.poll() is not None:
                        console.print(f"[yellow]child '{name}' exited (rc={proc.returncode})[/yellow]")
                for _ in range(refresh * 10):
                    if interrupted["flag"]:
                        break
                    time.sleep(0.1)

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
