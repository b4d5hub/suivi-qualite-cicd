import argparse
import os
import subprocess
import sys

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# DIRECTORY CONTAINING THIS COLLECTOR SCRIPT
COLLECTOR_DIR = Path(__file__).resolve().parent

# DIRECTORY WHERE BATCH LOG FILES ARE WRITTEN
LOG_DIR = COLLECTOR_DIR.parent.parent / "state"


def main():

    # DEFINE COMMAND-LINE OPTIONS
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=True,
        type=date.fromisoformat,
    )

    parser.add_argument(
        "--end",
        required=True,
        type=date.fromisoformat,
    )

    parser.add_argument(
        "--batch-days",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--max-batches",
        type=int,
        default=1,
    )

    args = parser.parse_args()

    # VALIDATE BATCH SIZES AND LIMITS
    if args.batch_days < 1 or args.max_batches < 1:
        parser.error("Batch sizes and limits must be positive")

    # ENFORCE A SENSIBLE DATE ORDER
    if args.start > args.end:
        parser.error("--start must not be after --end")

    # DO NOT ALLOW PROCESSING A DAY THAT IS NOT YET COMPLETE (UTC)
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

    if args.end > yesterday:
        parser.error("--end must be a completed UTC day")

    # MAKE SURE THE LOG DIRECTORY EXISTS
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    current = args.start

    # PROCESS BATCHES SEQUENTIALLY, ONE SUBPROCESS PER BATCH
    for batch_number in range(1, args.max_batches + 1):

        # STOP IF EVERY REQUESTED DAY HAS BEEN PROCESSED
        if current > args.end:
            break

        # CLAMP THE BATCH SIZE TO THE REMAINING DATE RANGE
        days = min(
            args.batch_days,
            (args.end - current).days + 1,
        )

        batch_end = current + timedelta(days=days - 1)

        # ONE LOG FILE PER BATCH, NAMED BY ITS DATE RANGE
        log_file = LOG_DIR / (f"backfill_{current}_{batch_end}.log")

        print(
            f"\nBatch {batch_number}: " f"{current} through {batch_end}",
            flush=True,
        )

        # BUILD THE COLLECTOR COMMAND
        command = [
            sys.executable,
            str(COLLECTOR_DIR / "backfill_range.py"),
            "--start",
            current.isoformat(),
            "--days",
            str(days),
        ]

        # FORCE UNBUFFERED OUTPUT FROM THE CHILD PROCESS
        environment = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
        }

        # RUN THE COLLECTOR AND STREAM ITS OUTPUT INTO THE LOG FILE
        with log_file.open("w", encoding="utf-8") as log:

            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=environment,
            )

            # LINES MATCHING THESE MARKERS ARE ECHOED TO THE CONSOLE
            summary_markers = (
                "Processing UTC day:",
                "BACKFILL SUMMARY",
                "Workflow runs:",
                "Messages delivered:",
                "Unchanged messages skipped:",
                "Date-range backfill completed successfully.",
                "Network error:",
                "GitHub rate limit reached.",
            )

            # STREAM EVERY LINE TO THE LOG, ECHO KEY LINES TO CONSOLE
            for line in process.stdout:
                log.write(line)

                if any(marker in line for marker in summary_markers):
                    print(line.rstrip(), flush=True)

            return_code = process.wait()

        print(f"Log saved: {log_file}")

        # FAIL FAST: PRINT THE TAIL OF THE LOG AND ABORT THE RUN
        if return_code != 0:
            print(f"FAILED: Batch {current} to {batch_end}")
            print("Last 15 log lines:")

            lines = log_file.read_text(encoding="utf-8").splitlines()

            for line in lines[-15:]:
                print(line)

            sys.exit(return_code)

        print("Batch completed successfully.")

        # ADVANCE TO THE FIRST DAY AFTER THIS BATCH
        current = batch_end + timedelta(days=1)

    # FINAL STATUS REPORT
    if current <= args.end:
        print(f"\nNext unprocessed date: {current}")
        print("Batch limit reached.")
    else:
        print("\nAll requested dates completed.")


if __name__ == "__main__":
    main()
