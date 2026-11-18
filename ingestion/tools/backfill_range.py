import _collector_path

import argparse
import os

from datetime import date, datetime, timedelta, timezone

from backfill_deployments import subtract_months
from workflows import backfill_day


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

        backfill_day(current)

        current += timedelta(days=1)

    print("\nDate-range backfill completed successfully.")


if __name__ == "__main__":
    main()
