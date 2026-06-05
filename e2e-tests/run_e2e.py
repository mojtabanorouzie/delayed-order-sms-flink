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
from dataclasses import dataclass

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FLINK_API = "http://localhost:8081"
FLINK_JAR_NAME = "delayed-order-sms-flink-job.jar"
FLINK_JAR_PATH = os.path.join(
    PROJECT_ROOT, "flink-job", "target", FLINK_JAR_NAME
)

TOPIC_SMS_COMMANDS = "sms-commands"

EXPECTED_SMS = {
    "delayed-orders": 5,
    "on-time-orders": 0,
    "cancelled-orders": 0,
    "duplicate-events": 5,  # NOT 10 — idempotent
    "eta-updated-orders": 5,
    "mixed-orders": (0, 2),  # 25% of 5 orders → 0-2 delayed (small-N variance)
}

SCENARIO_WAIT_SECONDS = {
    "delayed-orders": 15,
    "on-time-orders": 15,
    "cancelled-orders": 15,
    "duplicate-events": 15,
    "eta-updated-orders": 25,
    "mixed-orders": 25,
}


@dataclass
class E2EResult:
    scenario: str
    passed: bool
    expected: str
    actual_sms_count: int
    error: str | None = None


def run(cmd: list[str], cwd: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command with output captured."""
    return subprocess.run(
        cmd,
        cwd=cwd or PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def poll_healthy(timeout: int = 120) -> bool:
    """Poll `docker compose ps` until all services report healthy/running."""
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
    """Build the Flink fat JAR (requires JAVA_HOME pointing to JDK 17+)."""
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


def submit_flink_job() -> str | None:
    """Upload the JAR and start the Flink job. Returns the job-id or None."""
    jar_path = FLINK_JAR_PATH
    if not os.path.isfile(jar_path):
        # Try the cleaner jar name pattern
        candidates = [
            f for f in os.listdir(os.path.dirname(jar_path))
            if f.endswith(".jar") and "original" not in f
        ]
        if candidates:
            jar_path = os.path.join(os.path.dirname(jar_path), candidates[0])
        else:
            print(f"[ERROR] No JAR found in {os.path.dirname(jar_path)}")
            return None

    jar_name = os.path.basename(jar_path)
    upload_url = f"{FLINK_API}/jars/upload"

    print(f"[FLINK] Uploading {jar_name} ...")
    try:
        import requests
    except ImportError:
        # Fall back to subprocess + curl
        result = subprocess.run(
            [
                "curl", "-s", "-X", "POST",
                "-H", "Expect:",
                "-F", f"jarfile=@{jar_path}",
                upload_url,
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            print(f"[ERROR] Upload failed: {result.stderr}")
            return None
        resp = json.loads(result.stdout)
    else:
        with open(jar_path, "rb") as f:
            resp_raw = requests.post(upload_url, files={"jarfile": (jar_name, f)})
        if resp_raw.status_code != 200:
            print(f"[ERROR] Upload HTTP {resp_raw.status_code}: {resp_raw.text}")
            return None
        resp = resp_raw.json()

    jar_filename = resp.get("filename", "")
    if not jar_filename:
        jar_filename = jar_name

    # Find the jar id from the files list
    jar_id = None
    for part in resp.get("files", []):
        if part.get("name") == jar_filename:
            jar_id = part.get("id")
            break
    if not jar_id:
        # Try direct id in response
        jar_id = resp.get("filename", "").replace("/", "_")

    # Run the job
    run_url = f"{FLINK_API}/jars/{jar_id}/run"
    run_payload = {
        "entryClass": "com.company.delayedordersms.DelayedOrderSmsJob",
        "programArgs": (
            "--kafka.bootstrap.servers kafka:29092 "
            "--orders.topic Orders "
            "--sms.commands.topic sms-commands "
            "--consumer.group.id delayed-order-sms-flink "
            "--checkpoint.storage.path file:///opt/flink/checkpoints "
            "--parallelism 1"
        ),
    }

    print("[FLINK] Starting job...")
    try:
        import requests
    except ImportError:
        result = subprocess.run(
            [
                "curl", "-s", "-X", "POST",
                "-H", "Content-Type: application/json",
                "-d", json.dumps(run_payload),
                run_url,
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"[ERROR] Job submission failed: {result.stderr}")
            return None
        run_resp = json.loads(result.stdout)
    else:
        run_raw = requests.post(run_url, json=run_payload)
        if run_raw.status_code != 200:
            print(f"[ERROR] Job submission HTTP {run_raw.status_code}: {run_raw.text}")
            return None
        run_resp = run_raw.json()

    job_id = run_resp.get("jobid", "")
    print(f"  Job submitted: {job_id}")
    return job_id


def poll_job_running(job_id: str, timeout: int = 60) -> bool:
    """Poll Flink REST API until the job is RUNNING."""
    deadline = time.monotonic() + timeout
    url = f"{FLINK_API}/jobs/{job_id}"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url) as f:
                job = json.loads(f.read())
            state = job.get("state", "")
            if state == "RUNNING":
                print(f"  Job {job_id} is RUNNING")
                return True
            print(f"  Job state: {state}")
        except (urllib.error.URLError, json.JSONDecodeError) as exc:
            print(f"  Flink API not ready: {exc}")
        time.sleep(2)
    print(f"[ERROR] Job {job_id} did not reach RUNNING within {timeout}s")
    return False


def consume_sms_count(timeout_ms: int = 30000, max_messages: int = 100) -> int:
    """Consume messages from the sms-commands topic and count unique commandIds."""
    cmd = [
        "docker", "exec", "kafka",
        "bash", "-c",
        f"kafka-console-consumer "
        f"--bootstrap-server kafka:29092 "
        f"--topic {TOPIC_SMS_COMMANDS} "
        f"--from-beginning "
        f"--max-messages {max_messages} "
        f"--timeout-ms {timeout_ms} "
        f"2>&1",
    ]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
    command_ids = set()
    for line in lines:
        if "commandId" in line:
            try:
                obj = json.loads(line)
                cid = obj.get("commandId", "")
                if cid:
                    command_ids.add(cid)
            except json.JSONDecodeError:
                pass
        elif "Processed a total of" in line:
            # End marker, ignore
            continue
    return len(command_ids)


def run_scenario(name: str, orders_count: int = 5) -> subprocess.CompletedProcess:
    """Run a single scenario inside the Docker simulator service."""
    return subprocess.run(
        [
            "docker", "compose", "--profile", "e2e", "run", "--rm",
            "simulator",
            "--scenario", name,
            "--orders-count", str(orders_count),
            "--kafka-bootstrap-servers", "kafka:29092",
            "--orders-topic", "Orders",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )


def validate_sms_count(scenario: str, actual: int) -> bool:
    """Check whether the actual SMS count matches expectations."""
    expected = EXPECTED_SMS[scenario]
    if isinstance(expected, tuple):
        return expected[0] <= actual <= expected[1]
    return actual == expected


def expected_str(scenario: str) -> str:
    expected = EXPECTED_SMS[scenario]
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

    scenarios = [
        "delayed-orders",
        "on-time-orders",
        "cancelled-orders",
        "duplicate-events",
        "eta-updated-orders",
        "mixed-orders",
    ]

    results: list[E2EResult] = []

    # ---- Phase 1: Start infrastructure ----
    print("=" * 60)
    print("Phase 1: Starting infrastructure")
    print("=" * 60)

    # Tear down any previous run first
    subprocess.run(
        ["docker", "compose", "down", "-v"],
        cwd=PROJECT_ROOT, capture_output=True,
    )
    time.sleep(3)

    result = subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[ERROR] docker compose up failed:\n{result.stdout}\n{result.stderr}")
        return 1

    if not poll_healthy():
        return 1

    # Ensure Kafka topics exist (kafka-init may not have finished)
    print("[KAFKA] Creating topics...")
    topics_cmd = (
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic Orders --partitions 3 --replication-factor 1 "
        "--config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01 --config segment.ms=60000 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic sms-commands --partitions 3 --replication-factor 1 && "
        "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists "
        "--topic dead-letter-events --partitions 3 --replication-factor 1"
    )
    subprocess.run(
        ["docker", "exec", "kafka", "bash", "-c", topics_cmd],
        cwd=PROJECT_ROOT, capture_output=True,
    )
    print("  Topics ready")

    # Fix Flink checkpoint volume permissions (Docker volume mounts as root on Windows)
    print("[PERM] Fixing checkpoint volume permissions...")
    for container in ("flink-jobmanager", "flink-taskmanager"):
        chown_result = subprocess.run(
            ["docker", "exec", "-u", "root", container,
             "chown", "-R", "flink:flink", "/opt/flink/checkpoints", "/opt/flink/savepoints"],
            cwd=PROJECT_ROOT, capture_output=True, text=True,
        )
        if chown_result.returncode != 0:
            print(f"  chown {container} (non-fatal): {chown_result.stderr.strip()}")
        else:
            print(f"  {container} OK")

    # ---- Phase 2: Build & submit Flink job ----
    print("\n" + "=" * 60)
    print("Phase 2: Build & submit Flink job")
    print("=" * 60)

    if not build_flink_jar():
        return 1

    # Copy JAR into container at a non-bind-mount path to avoid permission issues
    jar_src = FLINK_JAR_PATH
    if not os.path.isfile(jar_src):
        candidates = [
            os.path.join(os.path.dirname(jar_src), f)
            for f in os.listdir(os.path.dirname(jar_src))
            if f.endswith(".jar") and "original" not in f
        ]
        if not candidates:
            print("[ERROR] No JAR found in flink-job/target/")
            return 1
        jar_src = candidates[0]

    print("[DOCKER] Copying JAR to jobmanager container...")
    subprocess.run(
        [
            "docker", "cp", jar_src,
            f"flink-jobmanager:/tmp/{FLINK_JAR_NAME}",
        ],
        cwd=PROJECT_ROOT, check=True,
    )

    # Submit via `flink run` inside the container
    print("[FLINK] Submitting job...")
    submit_result = subprocess.run(
        [
            "docker", "exec", "flink-jobmanager",
            "flink", "run", "-d",
            "-c", "com.company.delayedordersms.DelayedOrderSmsJob",
            f"/tmp/{FLINK_JAR_NAME}",
            "--kafka.bootstrap.servers", "kafka:29092",
            "--orders.topic", "Orders",
            "--sms.commands.topic", "sms-commands",
            "--consumer.group.id", "delayed-order-sms-flink",
            "--checkpoint.storage.path", "file:///opt/flink/checkpoints",
            "--parallelism", "1",
        ],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )

    stderr = (submit_result.stderr or "").strip()
    stdout = (submit_result.stdout or "").strip()

    # Extract job-id hex (32 chars) from combined output
    job_id = None
    for line in (stdout.splitlines() + stderr.splitlines()):
        line = line.strip()
        # WARNING lines sometimes contain hex-like tokens; prefer lines that are pure hex
        if line and len(line) >= 32:
            tokens = line.split()
            for token in tokens:
                token = token.rstrip(".")  # some output adds a trailing dot
                if len(token) == 32 and all(c in "0123456789abcdef" for c in token):
                    job_id = token
                    break
        if job_id:
            break

    if job_id:
        print(f"  Job submitted: {job_id}")
    else:
        print(f"  ERROR: Could not parse job ID from submit output:")
        for line in stdout.splitlines()[:10]:
            print(f"    stdout> {line}")
        for line in stderr.splitlines()[:10]:
            print(f"    stderr> {line}")
        print("  Continuing anyway...")

    time.sleep(5)  # Give Flink a moment to start

    # ---- Phase 3: Run scenarios ----
    print("\n" + "=" * 60)
    print("Phase 3: Running scenarios")
    print("=" * 60)

    previous_total = 0

    for scenario in scenarios:
        print(f"\n--- Scenario: {scenario} ---")
        print(f"  Running ...")
        scenario_result = run_scenario(scenario, orders_count=args.orders_count)
        if scenario_result.returncode != 0:
            print(f"  Scenario failed (exit {scenario_result.returncode})")
            print(f"  stderr: {scenario_result.stderr[:300]}")
            results.append(E2EResult(
                scenario=scenario, passed=False,
                expected=expected_str(scenario), actual_sms_count=-1,
                error=f"Simulator exit {scenario_result.returncode}: {scenario_result.stderr[:200]}",
            ))
            continue

        wait_sec = SCENARIO_WAIT_SECONDS.get(scenario, 15)
        print(f"  Waiting {wait_sec}s for processing...")
        time.sleep(wait_sec)

        print("  Consuming SMS from sms-commands topic...")
        raw_count = consume_sms_count(timeout_ms=30000)
        sms_count = raw_count - previous_total
        previous_total = raw_count

        passed = validate_sms_count(scenario, sms_count)
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] SMS count: {sms_count} (expected: {expected_str(scenario)})")

        results.append(E2EResult(
            scenario=scenario,
            passed=passed,
            expected=expected_str(scenario),
            actual_sms_count=sms_count,
        ))

    # ---- Phase 4: Report ----
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    header = f"{'Scenario':<25} {'Expected':<10} {'Actual':<8} {'Status':<8}"
    print(header)
    print("-" * len(header))

    all_passed = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.scenario:<25} {r.expected:<10} {r.actual_sms_count:<8} {status:<8}")
        if not r.passed and r.error:
            print(f"  Error: {r.error}")
        if not r.passed:
            all_passed = False

    passed_count = sum(1 for r in results if r.passed)
    print(f"\n{passed_count}/{len(results)} scenarios passed")

    # ---- Phase 5: Tear down ----
    if not args.no_cleanup:
        print("\n" + "=" * 60)
        print("Phase 5: Tearing down")
        print("=" * 60)
        subprocess.run(
            ["docker", "compose", "down", "-v"],
            cwd=PROJECT_ROOT, capture_output=True,
        )
        print("  Infrastructure removed")
    else:
        print("\nSkipping cleanup (--no-cleanup). Infrastructure still running.")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())