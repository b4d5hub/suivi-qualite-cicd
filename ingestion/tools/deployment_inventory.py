import _collector_path

import argparse, os
from datetime import datetime, timezone

from backfill_deployments import subtract_months
from github_client import list_deployments


def main():

    # DEFINE COMMAND-LINE OPTIONS
    parser = argparse.ArgumentParser()

    parser.add_argument("--max-pages", type=int, default=10)

    args = parser.parse_args()

    # REJECT NON-POSITIVE PAGE COUNTS UP FRONT
    if args.max_pages < 1:
        parser.error("--max-pages must be positive")

    # DETERMINE THE REQUESTED HISTORICAL PERIOD
    today = datetime.now(timezone.utc).date()
    months = int(os.getenv("BACKFILL_MONTHS", "2"))

    if months < 1:
        parser.error("BACKFILL_MONTHS must be positive")

    start = subtract_months(today, months)

    # TRACKING STATE FOR THE INVENTORY SCAN
    seen_ids = set()
    matched = 0
    missing_dates = 0
    complete = False

    print("DEPLOYMENT INVENTORY")
    print(f"UTC period: {start} to {today}")

    # SCAN UP TO max_pages DEPLOYMENT PAGES
    for page in range(1, args.max_pages + 1):

        deployments, metadata = list_deployments(per_page=100, page=page)

        # COLLECT CREATION DATES SEEN ON THIS PAGE
        dates = []

        for deployment in deployments:

            deployment_id = deployment["id"]

            # SKIP IDS ALREADY SEEN ON A PREVIOUS PAGE
            if deployment_id in seen_ids:
                continue

            seen_ids.add(deployment_id)

            created_at = deployment.get("created_at")

            # SKIP RECORDS WITHOUT A CREATION DATE
            if not created_at:
                missing_dates += 1
                continue

            day = created_at[:10]
            dates.append(day)

            # COUNT DEPLOYMENTS INSIDE THE REQUESTED WINDOW
            if start.isoformat() <= day <= today.isoformat():
                matched += 1

        print(
            f"Page {page}: "
            f"records={len(deployments)} | "
            f"oldest={min(dates) if dates else 'N/A'} | "
            f"newest={max(dates) if dates else 'N/A'} | "
            f"matched_total={matched} | "
            f"API_remaining={metadata['remaining']}"
        )

        # STOP EARLY IF GITHUB HAS NO NEXT PAGE
        if not metadata["has_next"]:
            complete = True
            break

    print("\n" + "=" * 50)
    print("INVENTORY SUMMARY")
    print("=" * 50)
    print(f"Unique deployments examined: {len(seen_ids)}")
    print(f"Deployments in period: {matched}")
    print(f"Missing creation dates: {missing_dates}")
    print(f"Pagination exhausted: {complete}")

    # WARN WHEN THE SCAN WAS TRUNCATED BY max_pages
    if not complete:
        print("INCOMPLETE: More pages remain.")


if __name__ == "__main__":
    main()
