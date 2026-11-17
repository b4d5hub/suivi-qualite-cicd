import argparse, calendar, os, subprocess, sys

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

# FIND THE REPOSITORY ROOT DIRECTORY
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# LOAD VARIABLES FROM THE ROOT .ENV FILE
load_dotenv(PROJECT_ROOT / ".env")

# DIRECTORY CONTAINING THIS SCRIPT (USED TO LOCATE THE COLLECTOR)
SCRIPT_DIR = Path(__file__).resolve().parent


def subtract_months(current_date, months):

    # CONVERT THE DATE TO A MONTH INDEX, THEN SHIFT BACK BY N MONTHS
    total_month = current_date.year * 12 + current_date.month - 1 - months

    # SPLIT THE INDEX BACK INTO YEAR AND 1-BASED MONTH
    year, zero_based_month = divmod(total_month, 12)
    month = zero_based_month + 1

    # CLAMP THE DAY SO IT FITS THE TARGET MONTH (E.G. JAN 31 -> FEB 28/29)
    day = min(current_date.day, calendar.monthrange(year, month)[1])

    return date(year, month, day)


def main():

    # DEFINE COMMAND-LINE OPTIONS
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start", type=date.fromisoformat, help="Optional start date: YYYY-MM-DD"
    )

    parser.add_argument(
        "--days", type=int, default=2, help="Maximum number of days to process"
    )

    args = parser.parse_args()

    # REJECT NON-POSITIVE DAY COUNTS UP FRONT
    if args.days < 1:
        parser.error("--days must be positive")

    # TODAY IN UTC (USED AS THE UPPER BOUND)
    today = datetime.now(timezone.utc).date()

    # DEFAULT BACKFILL LOOKBACK IN MONTHS (OVERRIDABLE VIA .ENV)
    months = int(os.getenv("BACKFILL_MONTHS", "2"))

    # COMPUTE THE EFFECTIVE DATE RANGE
    start = args.start or subtract_months(today, months)
    end = min(start + timedelta(days=args.days - 1), today - timedelta(days=1))

    if start > end:
        parser.error("Start date must be before today (UTC)")

    current = start

    # LOOP OVER EACH DAY IN THE RANGE AND RUN THE COLLECTOR
    while current <= end:

        print(f"\nProcessing UTC day: {current}", flush=True)

        # REUSE THE CHECKPOINTED COLLECTOR WE ALREADY BUILT
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "backfill_to_kafka.py"),
                "--date",
                current.isoformat(),
            ],
            check=True,
        )

        current += timedelta(days=1)

    print("\nDate-range backfill completed successfully.")


if __name__ == "__main__":
    main()
