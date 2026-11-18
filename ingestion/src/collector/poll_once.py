import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from poll_lock import polling_lock

COLLECTOR_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def run_collector(name, script, arguments=None):
    if arguments is None:
        arguments = []

    command = [
        sys.executable,
        "-u",
        str(COLLECTOR_DIR / script),
        *arguments,
    ]

    print("\n" + "=" * 60, flush=True)
    print(f"STARTING: {name}", flush=True)
    print("=" * 60, flush=True)

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
    )

    if result.returncode == 0:
        print(f"\nSUCCESS: {name}", flush=True)
    else:
        print(
            f"\nFAILED: {name} " f"(exit code {result.returncode})",
            flush=True,
        )

    return result.returncode == 0


def main():
    started_at = datetime.now(timezone.utc)

    print("=" * 60)
    print("GITHUB -> KAFKA INCREMENTAL POLLING")
    print("=" * 60)
    print(f"Started at: {started_at.isoformat()}")

    workflows_ok = run_collector(
        "WORKFLOWS -> BUILDS + TESTS",
        "workflows.py",
    )

    deployments_ok = run_collector(
        "DEPLOYMENTS",
        "deployments.py",
        ["--apply"],
    )

    print("\n" + "=" * 60)
    print("POLLING CYCLE SUMMARY")
    print("=" * 60)

    print(f"Workflows: " f"{'SUCCESS' if workflows_ok else 'FAILED'}")

    print(f"Deployments: " f"{'SUCCESS' if deployments_ok else 'FAILED'}")

    print(f"Finished at: " f"{datetime.now(timezone.utc).isoformat()}")

    if not workflows_ok or not deployments_ok:
        print("POLLING CYCLE FAILED")
        raise SystemExit(1)

    print("POLLING CYCLE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    try:
        with polling_lock():
            main()

    except BlockingIOError:
        print(
            "POLLING CYCLE BLOCKED: " "another cycle is already running.",
            file=sys.stderr,
        )
        raise SystemExit(2)
