import argparse
import os
import time

from datetime import datetime, timezone

from backfill_range import subtract_months
from backfill_deployments_preview import get_all_statuses

from github_client import list_deployments
from transform import transform_deployment
from kafka_producer import publish_messages

from state import (
    open_state,
    needs_publishing,
    remember_published,
)


def main():

    # DEFINE COMMAND-LINE OPTIONS
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-pages", type=int, default=3, help="Maximum pages to scan during a test"
    )

    parser.add_argument(
        "--all-pages", action="store_true", help="Scan until GitHub has no next page"
    )

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

    print("=" * 60)
    print("CHECKPOINTED DEPLOYMENT BACKFILL")
    print("=" * 60)
    print(f"UTC date range: {start} through {today}")

    # OPEN THE LOCAL CHECKPOINT DATABASE
    connection = open_state()

    # TRACKING STATE FOR THE SCAN
    seen_ids = set()

    examined = 0
    matched = 0
    delivered = 0
    skipped = 0
    duplicate_ids = 0
    missing_dates = 0
    pages_checked = 0

    all_pages_scanned = False
    page = 1

    try:

        # PAGINATE THROUGH ALL DEPLOYMENTS
        while True:

            deployments, metadata = list_deployments(per_page=100, page=page)

            pages_checked += 1
            examined += len(deployments)

            pending = []

            # PROCESS EACH DEPLOYMENT ON THE CURRENT PAGE
            for deployment in deployments:

                deployment_id = deployment["id"]

                # DETECT OVERLAPPING PAGES
                if deployment_id in seen_ids:
                    duplicate_ids += 1
                    continue

                seen_ids.add(deployment_id)

                created_at = deployment.get("created_at")

                # SKIP RECORDS WITHOUT A CREATION DATE
                if not created_at:
                    missing_dates += 1
                    continue

                created_day = created_at[:10]

                # SKIP DEPLOYMENTS OUTSIDE THE REQUESTED WINDOW
                if not (start.isoformat() <= created_day <= today.isoformat()):
                    continue

                matched += 1

                # RETRIEVE ALL AVAILABLE STATUSES
                statuses = get_all_statuses(deployment_id)

                record = transform_deployment(deployment, statuses)

                topic = "deployments"
                key = str(deployment_id)

                # KEEP ONLY NEW OR CHANGED RECORDS
                if needs_publishing(connection, topic, key, record):
                    pending.append((topic, key, record))
                else:
                    skipped += 1

                # AVOID SENDING REQUESTS TOO AGGRESSIVELY
                time.sleep(0.5)

            # PUBLISH PAGE RESULTS AND SAVE CHECKPOINTS
            if pending:
                publish_messages(pending)
                remember_published(connection, pending)

                delivered += len(pending)

            print(
                f"Page {page}: "
                f"examined={len(deployments)} | "
                f"matched_total={matched} | "
                f"new_or_changed={len(pending)}",
                flush=True,
            )

            # NO NEXT PAGE: PAGINATION EXHAUSTED
            if not metadata["has_next"]:
                all_pages_scanned = True
                break

            # STOP SAFELY WHEN RUNNING A LIMITED PREVIEW
            if not args.all_pages and page >= args.max_pages:
                break

            page += 1
            time.sleep(1)

    finally:
        # ALWAYS CLOSE THE CHECKPOINT CONNECTION
        connection.close()

    # BASIC COVERAGE CHECKS BEFORE DECLARING SUCCESS
    coverage_check = all_pages_scanned and duplicate_ids == 0 and missing_dates == 0

    print("\n" + "=" * 60)
    print("DEPLOYMENT BACKFILL SUMMARY")
    print("=" * 60)

    print(f"Pages checked: {pages_checked}")
    print(f"Deployments examined: {examined}")
    print(f"Deployments matching period: {matched}")
    print(f"Messages delivered: {delivered}")
    print(f"Unchanged messages skipped: {skipped}")
    print(f"Duplicate IDs observed: {duplicate_ids}")
    print(f"Missing creation dates: {missing_dates}")
    print(f"Full list scanned: {all_pages_scanned}")
    print(f"Basic coverage checks passed: {coverage_check}")

    # WARN WHEN COVERAGE COULD NOT BE VERIFIED
    if not coverage_check:
        print(
            "WARNING: Coverage is not yet verified. "
            "Do not mark the backfill complete."
        )


if __name__ == "__main__":
    main()
