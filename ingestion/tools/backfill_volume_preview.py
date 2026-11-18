import _collector_path

import os
import time
from datetime import datetime, timedelta, timezone

from backfill_deployments import subtract_months
from github_client import list_workflow_runs


def main():

    # COMPUTE THE UTC DATE RANGE TO PREVIEW
    today = datetime.now(timezone.utc).date()
    months = int(os.getenv("BACKFILL_MONTHS", "2"))

    if months < 1:
        raise ValueError("BACKFILL_MONTHS must be positive")

    start = subtract_months(today, months)
    end = today - timedelta(days=1)

    # SCAN STATE
    current = start
    total_runs = 0
    days_checked = 0
    days_to_split = []

    print("=" * 60)
    print("TWO-MONTH WORKFLOW VOLUME PREVIEW")
    print("=" * 60)
    print(f"UTC period: {start} through {end}")

    # LOOP OVER EVERY DAY IN THE RANGE
    while current <= end:

        day = current.isoformat()

        # ONE-ITEM QUERY IS ENOUGH TO GET total_count FOR THE DAY
        data, metadata = list_workflow_runs(per_page=1, page=1, created=f"{day}..{day}")

        count = data.get("total_count")

        # REJECT MALFORMED COUNTS FROM THE API
        if not isinstance(count, int) or count < 0:
            raise RuntimeError(f"Invalid API count for {day}: {count}")

        total_runs += count
        days_checked += 1

        # FLAG DAYS THAT HIT GITHUB'S 1,000-RESULT CAP
        if count >= 1000:
            days_to_split.append(day)
            flag = " | SPLIT REQUIRED"
        else:
            flag = ""

        print(
            f"{day}: {count} runs{flag} | " f"API remaining: {metadata['remaining']}",
            flush=True,
        )

        current += timedelta(days=1)

        # SMALL PAUSE BETWEEN API REQUESTS
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("VOLUME PREVIEW SUMMARY")
    print("=" * 60)
    print(f"Days checked: {days_checked}")
    print(f"Reported workflow runs: {total_runs}")
    print(f"Days requiring smaller windows: {len(days_to_split)}")
    print(f"Dates to split: {days_to_split}")


if __name__ == "__main__":
    main()
