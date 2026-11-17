from datetime import datetime, timedelta, timezone

import requests

from github_client import list_workflow_runs


def collect_one_day(target_date):

    # CONVERT TARGET DATE TO ISO FORMAT FOR THE GITHUB QUERY
    day = target_date.isoformat()

    # BUILD THE GITHUB DATE-RANGE FILTER
    created_filter = f"{day}..{day}"

    # INITIALIZE PAGINATION STATE
    all_runs = []
    page = 1

    print("=" * 60)
    print("GITHUB HISTORICAL COLLECTION - ONE DAY")
    print("=" * 60)
    print(f"Target day (UTC): {day}")

    # RETRIEVE PAGES OF WORKFLOW RUNS UNTIL NONE LEFT
    while True:

        data, metadata = list_workflow_runs(
            per_page=100, page=page, created=created_filter
        )

        # TOTAL COUNT REPORTED BY GITHUB FOR THIS QUERY
        total_count = data.get("total_count", 0)

        # AVOID SILENTLY ACCEPTING GITHUB'S 1,000-RESULT CAP
        if total_count >= 1000:
            raise RuntimeError(
                "Date window may exceed GitHub's 1,000-result "
                "search limit. Split it into smaller time windows."
            )

        runs = data.get("workflow_runs", [])

        # ACCUMULATE RUNS FROM THE CURRENT PAGE
        all_runs.extend(runs)

        print(
            f"Page {page}: {len(runs)} runs | "
            f"API remaining: {metadata['remaining']}"
        )

        # STOP WHEN NO RUNS RETURNED OR NO NEXT PAGE EXISTS
        if not runs or not metadata["has_next"]:
            break

        page += 1

    # CHECK RECORD UNIQUENESS
    unique_ids = {run["id"] for run in all_runs}

    print("\n" + "=" * 60)
    print("HISTORICAL COLLECTION SUMMARY")
    print("=" * 60)

    print(f"GitHub reported: {total_count}")
    print(f"Runs collected: {len(all_runs)}")
    print(f"Unique run IDs: {len(unique_ids)}")

    if len(unique_ids) != len(all_runs):
        raise RuntimeError("Duplicate workflow run IDs detected.")

    if len(all_runs) != total_count:
        raise RuntimeError(
            f"Incomplete collection: expected {total_count}, "
            f"received {len(all_runs)}."
        )

    # COUNT WORKFLOW TYPES
    workflow_counts = {}

    for run in all_runs:
        name = run.get("name") or "Unknown"

        workflow_counts[name] = workflow_counts.get(name, 0) + 1

    print("\nWorkflows found:")

    for name, count in sorted(workflow_counts.items()):
        print(f"{name}: {count}")

    return all_runs


def main():

    # SELECT A COMPLETE DAY, THREE DAYS BEFORE TODAY (UTC)
    target_date = datetime.now(timezone.utc).date() - timedelta(days=3)

    try:

        collect_one_day(target_date)

    except (requests.RequestException, RuntimeError) as error:

        print(f"Collection failed: {error}")


if __name__ == "__main__":
    main()
