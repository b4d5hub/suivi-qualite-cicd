import argparse
from datetime import date

from github_client import (
    list_deployments,
    list_deployment_statuses,
)
from transform import transform_deployment


def get_all_statuses(deployment_id):

    # ACCUMULATED STATUS RECORDS FOR THIS DEPLOYMENT
    statuses = []
    page = 1

    # RETRIEVE EVERY AVAILABLE STATUS PAGE
    while True:

        batch, metadata = list_deployment_statuses(
            deployment_id=deployment_id,
            per_page=100,
            page=page,
        )

        # ACCUMULATE STATUSES FROM THE CURRENT PAGE
        statuses.extend(batch)

        # STOP WHEN NO NEXT PAGE EXISTS
        if not metadata["has_next"]:
            break

        page += 1

    return statuses


def preview_deployments(target_date, max_pages=3):

    # CONVERT TARGET DATE TO ISO FORMAT FOR COMPARISON
    day = target_date.isoformat()

    # PREVIEW STATE
    matched = {}
    pages_checked = 0
    examined = 0
    scan_complete = False

    print("=" * 60)
    print("GITHUB DEPLOYMENT HISTORY PREVIEW")
    print("=" * 60)
    print(f"Target day (UTC): {day}")

    # SCAN UP TO max_pages DEPLOYMENT PAGES
    for page in range(1, max_pages + 1):

        deployments, metadata = list_deployments(
            per_page=100,
            page=page,
        )

        pages_checked += 1
        examined += len(deployments)

        # KEEP ONLY DEPLOYMENTS CREATED ON THE TARGET DAY
        for deployment in deployments:

            created_at = deployment.get("created_at")

            if created_at and created_at[:10] == day:
                matched[deployment["id"]] = deployment

        print(
            f"Page {page}: {len(deployments)} examined | "
            f"Matching so far: {len(matched)}"
        )

        # STOP EARLY IF GITHUB HAS NO NEXT PAGE
        if not metadata["has_next"]:
            scan_complete = True
            break

    print("\nMatching deployments:")

    total_statuses = 0
    messages = []

    # FETCH STATUSES AND TRANSFORM EACH MATCHING DEPLOYMENT
    for deployment in matched.values():

        statuses = get_all_statuses(deployment["id"])

        message = transform_deployment(
            deployment,
            statuses,
        )
        messages.append(message)

        total_statuses += len(statuses)

        print(
            f"ID={message['deployment_id']} | "
            f"environment={message['environment']} | "
            f"latest_status={message['latest_status']} | "
            f"statuses={len(statuses)} | "
            f"production={message['production_environment']}"
        )

    print("\n" + "=" * 60)
    print("DEPLOYMENT PREVIEW SUMMARY")
    print("=" * 60)
    print(f"Pages checked: {pages_checked}")
    print(f"Deployments examined: {examined}")
    print(f"Deployments matching day: {len(matched)}")
    print(f"Status records retrieved: {total_statuses}")
    print(f"Full list scanned: {scan_complete}")

    # WARN WHEN THE SCAN WAS TRUNCATED BY max_pages
    if not scan_complete:
        print(
            "PREVIEW ONLY: More deployment pages exist. "
            "Results are not guaranteed complete."
        )
    return messages, scan_complete


def main():

    # DEFINE COMMAND-LINE OPTIONS
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        required=True,
        type=date.fromisoformat,
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    # REJECT NON-POSITIVE PAGE COUNTS UP FRONT
    if args.max_pages < 1:
        parser.error("--max-pages must be positive")

    preview_deployments(args.date, args.max_pages)


if __name__ == "__main__":
    main()
