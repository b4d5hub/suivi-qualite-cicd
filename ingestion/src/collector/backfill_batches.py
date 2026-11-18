import argparse
import sys
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "state"

SUMMARY_MARKERS = (
    "Processing UTC day:",
    "BACKFILL SUMMARY",
    "Workflow runs:",
    "Messages delivered:",
    "Unchanged messages skipped:",
    "Date-range backfill completed successfully.",
    "Network error:",
    "GitHub rate limit reached.",
)


class BatchLog:
    def __init__(self, log, console):
        self.log = log
        self.console = console
        self.pending = ""

    def write(self, text):
        self.log.write(text)
        lines = (self.pending + text).split("\n")
        self.pending = lines.pop()
        for line in lines:
            if any(marker in line for marker in SUMMARY_MARKERS):
                print(line, file=self.console, flush=True)
        return len(text)

    def flush(self):
        self.log.flush()


def run_batch(start, end, log_file):
    with log_file.open("w", encoding="utf-8", errors="replace") as log:
        output = BatchLog(log, sys.stdout)
        with redirect_stdout(output), redirect_stderr(output):
            try:
                from workflows import backfill_day

                current = start
                while current <= end:
                    print(f"\nProcessing UTC day: {current}", flush=True)
                    backfill_day(current)
                    current += timedelta(days=1)
                print("\nDate-range backfill completed successfully.")
            except Exception:
                traceback.print_exc(file=log)
                return False
    return True


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

    # Process batches sequentially so a retry can resume from the failed batch.
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

        completed = run_batch(current, batch_end, log_file)
        print(f"Log saved: {log_file}")

        # FAIL FAST: PRINT THE TAIL OF THE LOG AND ABORT THE RUN
        if not completed:
            print(f"FAILED: Batch {current} to {batch_end}")
            print("Last 15 log lines:")

            lines = log_file.read_text(encoding="utf-8").splitlines()

            for line in lines[-15:]:
                print(line)

            sys.exit(1)

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
