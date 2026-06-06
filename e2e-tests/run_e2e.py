"""End-to-end test automation for the delayed-order-sms-flink pipeline.

Usage:
    python e2e-tests/run_e2e.py              # Full run, clean up afterwards
    python e2e-tests/run_e2e.py --no-cleanup # Leave infrastructure running
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FLINK_API = "http://localhost:8081"
FLINK_JAR_NAME = "delayed-order-sms-flink-job.jar"
FLINK_JAR_PATH = os.path.join(
    PROJECT_ROOT, "flink-job", "target", FLINK_JAR_NAME
)

TOPIC_SMS_COMMANDS = "sms-commands"
TOPIC_REFUND_COMMANDS = "refund-commands"
TOPIC_COURIER_COMMANDS = "courier-pause-commands"
TOPIC_RESTAURANT_ALERTS = "restaurant-alerts"
TOPIC_SURGE_SIGNALS = "surge-pricing-signals"

# Expected counts per scenario per output topic.
# Values are int (exact) or tuple (inclusive range).
EXPECTED_SMS = {
    "delayed-orders": 5,
    "on-time-orders": 0,
    "cancelled-orders": 0,
    "duplicate-events": 5,   # idempotent — NOT 10
    "eta-updated-orders": 5,
    "mixed-orders": (0, 2),  # 25% of 5 → small-N variance
}

EXPECTED_REFUNDS = {
    "severely-delayed-refund": 5,
}

EXPECTED_COURIER_PAUSES = {
    # At least 1 PAUSE when 8+ active orders for the same courier
    "courier-overload": (1, 1),
}

EXPECTED_RESTAURANT_ALERTS = {
    # At least 1 CRITICAL alert when avg pickup > 15 min in the window
    "restaurant-bottleneck": (1, 5),
}

EXPECTED_SURGE_SIGNALS = {
    # 1 SURGE_PRICING signal: demandFactor=1.0, weatherFactor=1.2(RAIN) → multiplier=1.6 > 1.15 threshold
    "surge-pricing": (1, 3),
}

SCENARIO_WAIT_SECONDS = {
    "delayed-orders": 15,
    "on-time-orders": 15,
    "cancelled-orders": 15,
    "duplicate-events": 15,
    "eta-updated-orders": 25,
    "mixed-orders": 25,
    "severely-delayed-refund": 15,
    "courier-overload": 15,
    "restaurant-bottleneck": 30,  # window.size.seconds=10 + processing headroom
    "surge-pricing": 30,          # window.size.seconds=10 + processing headroom
}


@dataclass
class E2EResult:
    scenario: str
    passed: bool
    checks: dict[str, str] = field(default_factory=dict)   # topic → "actual (expected)"
    error: str | None = None


def run(cmd: list[str], cwd: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=cwd or PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def poll_healthy(timeout: int = 120) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"  docker compose ps failed (exit {result.returncode})")
            time.sleep(2)
            continue

        lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
        if not lines:
            print("  No containers yet...")
            time.sleep(2)
            continue

        all_ok = True
        for line in lines:
            try:
                container = json.loads(line)
            except json.JSONDecodeError:
                continue
            state = container.get("State", "")
            health = container.get("Health", "")
            name = container.get("Name", "unknown")
            is_ok = state in ("running",) and health in ("healthy", "")
            if not is_ok:
                print(f"  Waiting: {name}  state={state}  health={health}")
                all_ok = False

        if all_ok:
            print("  All services ready")
            return True

        time.sleep(2)

    print(f"[ERROR] Services did not become healthy within {timeout}s")
    return False


def build_flink_jar() -> bool:
    print("[BUILD] Compiling Flink job...")
    mvn_path = shutil.which("mvn")
    if not mvn_path:
        print("[ERROR] mvn not found on PATH")
        return False
    env = os.environ.copy()
    java_home_candidates = [
        os.environ.get("JAVA_HOME", ""),
        r"C:\Program Files\Java\jdk-17.0.2",
        r"C:\Program Files\Java\latest",
    ]
    for candidate in java_home_candidates:
        if candidate and os.path.isdir(candidate):
            env["JAVA_HOME"] = candidate
            env["PATH"] = os.path.join(candidate, "bin") + os.pathsep + env.get("PATH", "")
            break
    result = subprocess.run(
        [
            mvn_path, "clean", "package", "-f",
            os.path.join(PROJECT_ROOT, "flink-job", "pom.xml"),
            "-DskipTests", "-q",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        print(f"[ERROR] Build failed:\n{result.stdout}\n{result.stderr}")
        return False
    print("  Build succeeded")
    return True


def copy_jar_to_container() -> str | None:
    """Copy the fat JAR into the jobmanager container. Returns the in-container path or None."""
    jar_src = FLINK_JAR_PATH
    if not os.path.isfile(jar_src):
        candidates = [
            os.path.join(os.path.dirname(jar_src), f)
            for f in os.listdir(os.path.dirname(jar_src))
            if f.endswith(".jar") and "original" not in f
        ]
        if not candidates:
            print("[ERROR] No JAR found in flink-job/target/")
            return None
        jar_src = candidates[0]

    container_path = f"/tmp/{FLINK_JAR_NAME}"
    print("[DOCKER] Copying JAR to jobmanager container...")
    result = subprocess.run(
        ["docker", "cp", jar_src, f"flink-jobmanager:{container_path}"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[ERROR] docker cp failed: {result.stderr}")
        return None
    return container_path


def submit_job(entry_class: str, program_args: list[str], label: str) -> str | None:
    """Submit a Flink job inside the container. Returns job-id hex or None."""
    container_jar = f"/tmp/{FLINK_JAR_NAME}"
    cmd = [
        "docker", "exec", "flink-jobmanager",
        "flink", "run", "-d",
        "-c", entry_class,
        container_jar,
    ] + program_args

    print(f"[FLINK] Submitting {label}...")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()

    job_id = None
    for line in (stdout.splitlines() + stderr.splitlines()):
        line = line.strip()
        if line and len(line) >= 32:
            for token in line.split():
                token = token.rstrip(".")
                if len(token) == 32 and all(c in "0123456789abcdef" for c in token):
                    job_id = token
                    break
        if job_id:
            break

    if job_id:
        print(f"  {label} submitted: {job_id}")
    else:
        print(f"  WARNING: Could not parse job ID for {label}")
        for line in stdout.splitlines()[:5]:
            print(f"    stdout> {line}")
        for line in stderr.splitlines()[:5]:
            print(f"    stderr> {line}")
    return job_id


def consume_topic_count(topic: str, timeout_ms: int = 30000, max_messages: int = 200) -> int:
    """Consume from a topic and return the number of unique commandId / alertId values."""
    cmd = [
        "docker", "exec", "kafka",
        "bash", "-c",
        f"kafka-console-consumer "
        f"--bootstrap-server kafka:29092 "
        f"--topic {topic} "
        f"--from-beginning "
        f"--max-messages {max_messages} "
        f"--timeout-ms {timeout_ms} "
        f"2>&1",
    ]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
    unique_ids: set[str] = set()
    for line in lines:
        if "{" not in line:
            continue
        try:
            obj = json.loads(line)
            uid = obj.get("commandId") or obj.get("alertId") or obj.get("signalId") or ""
            if uid:
                unique_ids.add(uid)
        except json.JSONDecodeError:
            pass
    return len(unique_ids)


def run_scenario(name: str, orders_count: int = 5, topic: str = "Orders") -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            "docker", "compose", "--profile", "e2e", "run", "--rm",
            "simulator",
            "--scenario", name,
            "--orders-count", str(orders_count),
            "--kafka-bootstrap-servers", "kafka:29092",
            "--orders-topic", topic,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )


def inject_weather(region: str, condition: str) -> bool:
    """Produce a single weather JSON event to the weather-data topic."""
    from datetime import datetime, timezone
    weather_json = json.dumps({
        "region": region,
        "condition": condition,
        "temperature": 15.0,
        "windSpeed": 10.0,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schemaVersion": 1,
    })
    cmd = [
        "docker", "exec", "kafka", "bash", "-c",
        f"echo '{weather_json}' | kafka-console-producer "
        "--bootstrap-server kafka:29092 "
        "--topic weather-data",
    ]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=15)
    return result.returncode == 0


def validate_count(actual: int, expected: int | tuple[int, int]) -> bool:
    if isinstance(expected, tuple):
        return expected[0] <= actual <= expected[1]
    return actual == expected


def expected_str(expected: int | tuple[int, int]) -> str:
    if isinstance(expected, tuple):
        return f"{expected[0]}-{expected[1]}"
    return str(expected)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run E2E tests for delayed-order-sms-flink")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Leave infrastructure running after tests")
    parser.add_argument("--orders-count", type=int, default=5,
                        help="Orders per scenario (default: 5)")
    args = parser.parse_args()

    results: list[E2EResult] = []

    # ── Phase 1: Infrastructure ──────────────────────────────────────
    print("=" * 60)
    print("Phase 1: Starting infrastructure")
    print("=" * 60)

    subprocess.run(["docker", "compose", "down", "-v"], cwd=PROJECT_ROOT, capture_output=True)
    time.sleep(3)

    up = subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    if up.returncode != 0:
        print(f"[ERROR] docker compose up failed:\n{up.stdout}\n{up.stderr}")
        return 1

    if not poll_healthy():
        return 1

    print("[KAFKA] Creating topics...")
    topics_cmd = (
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic Orders --partitions 3 --replication-factor 1 "
        "--config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01 --config segment.ms=60000 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic sms-commands --partitions 3 --replication-factor 1 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic dead-letter-events --partitions 3 --replication-factor 1 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic refund-commands --partitions 8 --replication-factor 1 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic courier-pause-commands --partitions 8 --replication-factor 1 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic restaurant-alerts --partitions 4 --replication-factor 1 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic weather-data --partitions 4 --replication-factor 1 "
        "--config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01 --config segment.ms=60000 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic surge-pricing-signals --partitions 8 --replication-factor 1"
    )
    subprocess.run(
        ["docker", "exec", "kafka", "bash", "-c", topics_cmd],
        cwd=PROJECT_ROOT, capture_output=True,
    )
    print("  Topics ready")

    print("[PERM] Fixing checkpoint volume permissions...")
    for container in ("flink-jobmanager", "flink-taskmanager"):
        chown = subprocess.run(
            ["docker", "exec", "-u", "root", container,
             "chown", "-R", "flink:flink", "/opt/flink/checkpoints", "/opt/flink/savepoints"],
            cwd=PROJECT_ROOT, capture_output=True, text=True,
        )
        status = "OK" if chown.returncode == 0 else f"(non-fatal: {chown.stderr.strip()})"
        print(f"  {container} {status}")

    # ── Phase 2: Build & submit all Flink jobs ──────────────────────
    print("\n" + "=" * 60)
    print("Phase 2: Build & submit Flink jobs")
    print("=" * 60)

    if not build_flink_jar():
        return 1

    if not copy_jar_to_container():
        return 1

    base_args = [
        "--kafka.bootstrap.servers", "kafka:29092",
        "--orders.topic", "Orders",
        "--checkpoint.storage.path", "file:///opt/flink/checkpoints",
        "--parallelism", "1",
    ]

    submit_job(
        "com.company.delayedordersms.DelayedOrderSmsJob",
        base_args + ["--sms.commands.topic", "sms-commands",
                     "--consumer.group.id", "delayed-order-sms-flink"],
        "DelayedOrderSmsJob",
    )
    submit_job(
        "com.company.delayedordersms.AutoRefundJob",
        base_args + ["--refund.commands.topic", "refund-commands",
                     "--consumer.group.id", "auto-refund-flink",
                     "--refund.delay.minutes", "0"],
        "AutoRefundJob",
    )
    submit_job(
        "com.company.delayedordersms.CourierOverloadJob",
        base_args + ["--courier.commands.topic", "courier-pause-commands",
                     "--consumer.group.id", "courier-overload-flink",
                     "--overload.threshold", "8",
                     "--resume.threshold", "5"],
        "CourierOverloadJob",
    )
    submit_job(
        "com.company.delayedordersms.RestaurantBottleneckJob",
        base_args + ["--restaurant.alerts.topic", "restaurant-alerts",
                     "--consumer.group.id", "restaurant-bottleneck-flink",
                     "--window.size.seconds", "10",
                     "--alert.threshold.minutes", "15",
                     "--baseline.minutes", "8"],
        "RestaurantBottleneckJob",
    )
    submit_job(
        "com.company.delayedordersms.SurgePricingJob",
        base_args + ["--surge.signals.topic", "surge-pricing-signals",
                     "--consumer.group.id", "surge-pricing-flink",
                     "--window.size.seconds", "10",
                     "--at.risk.threshold.minutes", "25",
                     "--surge.threshold", "1.15",
                     "--demand.weight", "0.5"],
        "SurgePricingJob",
    )

    time.sleep(8)  # Let all five jobs reach RUNNING state

    # ── Phase 3: SMS scenarios ───────────────────────────────────────
    print("\n" + "=" * 60)
    print("Phase 3: SMS scenarios")
    print("=" * 60)

    sms_scenarios = [
        "delayed-orders",
        "on-time-orders",
        "cancelled-orders",
        "duplicate-events",
        "eta-updated-orders",
        "mixed-orders",
    ]

    prev_sms = 0
    for scenario in sms_scenarios:
        print(f"\n--- {scenario} ---")
        sr = run_scenario(scenario, orders_count=args.orders_count)
        if sr.returncode != 0:
            results.append(E2EResult(scenario=scenario, passed=False,
                                     error=f"Simulator exit {sr.returncode}: {sr.stderr[:200]}"))
            continue

        wait = SCENARIO_WAIT_SECONDS.get(scenario, 15)
        print(f"  Waiting {wait}s...")
        time.sleep(wait)

        raw = consume_topic_count(TOPIC_SMS_COMMANDS)
        actual = raw - prev_sms
        prev_sms = raw

        expected = EXPECTED_SMS[scenario]
        passed = validate_count(actual, expected)
        print(f"  [{'PASS' if passed else 'FAIL'}] sms-commands: {actual} "
              f"(expected {expected_str(expected)})")
        results.append(E2EResult(
            scenario=scenario,
            passed=passed,
            checks={"sms-commands": f"{actual} (expected {expected_str(expected)})"},
        ))

    # ── Phase 4: Refund scenario ─────────────────────────────────────
    print("\n" + "=" * 60)
    print("Phase 4: Auto-refund scenario")
    print("=" * 60)

    prev_refund = 0
    for scenario, expected in EXPECTED_REFUNDS.items():
        print(f"\n--- {scenario} ---")
        sr = run_scenario(scenario, orders_count=args.orders_count)
        if sr.returncode != 0:
            results.append(E2EResult(scenario=scenario, passed=False,
                                     error=f"Simulator exit {sr.returncode}: {sr.stderr[:200]}"))
            continue

        wait = SCENARIO_WAIT_SECONDS.get(scenario, 15)
        print(f"  Waiting {wait}s...")
        time.sleep(wait)

        raw = consume_topic_count(TOPIC_REFUND_COMMANDS)
        actual = raw - prev_refund
        prev_refund = raw

        passed = validate_count(actual, expected)
        print(f"  [{'PASS' if passed else 'FAIL'}] refund-commands: {actual} "
              f"(expected {expected_str(expected)})")
        results.append(E2EResult(
            scenario=scenario,
            passed=passed,
            checks={"refund-commands": f"{actual} (expected {expected_str(expected)})"},
        ))

    # ── Phase 5: Courier overload scenario ───────────────────────────
    print("\n" + "=" * 60)
    print("Phase 5: Courier overload scenario")
    print("=" * 60)

    prev_courier = 0
    for scenario, expected in EXPECTED_COURIER_PAUSES.items():
        print(f"\n--- {scenario} ---")
        sr = run_scenario(scenario, orders_count=args.orders_count)
        if sr.returncode != 0:
            results.append(E2EResult(scenario=scenario, passed=False,
                                     error=f"Simulator exit {sr.returncode}: {sr.stderr[:200]}"))
            continue

        wait = SCENARIO_WAIT_SECONDS.get(scenario, 15)
        print(f"  Waiting {wait}s...")
        time.sleep(wait)

        raw = consume_topic_count(TOPIC_COURIER_COMMANDS)
        actual = raw - prev_courier
        prev_courier = raw

        passed = validate_count(actual, expected)
        print(f"  [{'PASS' if passed else 'FAIL'}] courier-pause-commands: {actual} "
              f"(expected {expected_str(expected)})")
        results.append(E2EResult(
            scenario=scenario,
            passed=passed,
            checks={"courier-pause-commands": f"{actual} (expected {expected_str(expected)})"},
        ))

    # ── Phase 6: Restaurant bottleneck scenario ───────────────────────
    print("\n" + "=" * 60)
    print("Phase 6: Restaurant bottleneck scenario")
    print("=" * 60)

    prev_alerts = 0
    for scenario, expected in EXPECTED_RESTAURANT_ALERTS.items():
        print(f"\n--- {scenario} ---")
        sr = run_scenario(scenario, orders_count=args.orders_count)
        if sr.returncode != 0:
            results.append(E2EResult(scenario=scenario, passed=False,
                                     error=f"Simulator exit {sr.returncode}: {sr.stderr[:200]}"))
            continue

        wait = SCENARIO_WAIT_SECONDS.get(scenario, 30)
        print(f"  Waiting {wait}s for window timer to fire...")
        time.sleep(wait)

        raw = consume_topic_count(TOPIC_RESTAURANT_ALERTS)
        actual = raw - prev_alerts
        prev_alerts = raw

        passed = validate_count(actual, expected)
        print(f"  [{'PASS' if passed else 'FAIL'}] restaurant-alerts: {actual} "
              f"(expected {expected_str(expected)})")
        results.append(E2EResult(
            scenario=scenario,
            passed=passed,
            checks={"restaurant-alerts": f"{actual} (expected {expected_str(expected)})"},
        ))

    # ── Phase 7: Surge pricing scenario ─────────────────────────────
    print("\n" + "=" * 60)
    print("Phase 7: Dynamic surge pricing scenario")
    print("=" * 60)

    prev_signals = 0
    for scenario, expected in EXPECTED_SURGE_SIGNALS.items():
        print(f"\n--- {scenario} ---")

        print("  Injecting RAIN weather for zone-surge-e2e...")
        if not inject_weather("zone-surge-e2e", "RAIN"):
            results.append(E2EResult(scenario=scenario, passed=False,
                                     error="Failed to inject weather event"))
            continue
        time.sleep(2)  # Give the job time to receive and store the weather event

        sr = run_scenario(scenario, orders_count=args.orders_count)
        if sr.returncode != 0:
            results.append(E2EResult(scenario=scenario, passed=False,
                                     error=f"Simulator exit {sr.returncode}: {sr.stderr[:200]}"))
            continue

        wait = SCENARIO_WAIT_SECONDS.get(scenario, 30)
        print(f"  Waiting {wait}s for window timer to fire...")
        time.sleep(wait)

        raw = consume_topic_count(TOPIC_SURGE_SIGNALS)
        actual = raw - prev_signals
        prev_signals = raw

        passed = validate_count(actual, expected)
        print(f"  [{'PASS' if passed else 'FAIL'}] surge-pricing-signals: {actual} "
              f"(expected {expected_str(expected)})")
        results.append(E2EResult(
            scenario=scenario,
            passed=passed,
            checks={"surge-pricing-signals": f"{actual} (expected {expected_str(expected)})"},
        ))

    # ── Phase 8: Results ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    header = f"{'Scenario':<30} {'Status':<8} {'Details'}"
    print(header)
    print("-" * 70)

    all_passed = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        details = " | ".join(r.checks.values()) if r.checks else (r.error or "")
        print(f"{r.scenario:<30} {status:<8} {details}")
        if not r.passed:
            all_passed = False

    passed_count = sum(1 for r in results if r.passed)
    print(f"\n{passed_count}/{len(results)} scenarios passed")

    # ── Phase 9: Teardown ────────────────────────────────────────────
    if not args.no_cleanup:
        print("\n" + "=" * 60)
        print("Phase 9: Tearing down")
        print("=" * 60)
        subprocess.run(["docker", "compose", "down", "-v"], cwd=PROJECT_ROOT, capture_output=True)
        print("  Infrastructure removed")
    else:
        print("\nSkipping cleanup (--no-cleanup). Infrastructure still running.")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
