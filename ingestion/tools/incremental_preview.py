import _collector_path

from datetime import datetime, timedelta, timezone

from github_client import list_workflow_runs


def main():

    # COMPUTE THE 24-HOUR LOOKBACK WINDOW (UTC)
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=24)

    # BUILD THE GITHUB DATE-RANGE FILTER (DATE-LEVEL GRANULARITY ONLY)
    created_filter = f"{window_start.date()}..{now.date()}"

    print("=" * 60)
    print("INCREMENTAL WORKFLOW PREVIEW")
    print("=" * 60)
    print(f"UTC time: {now.isoformat()}")
    print(f"Recent window starts: {window_start.isoformat()}")
    print(f"GitHub created filter: {created_filter}")

    # PAGINATION AND DEDUPLICATION STATE
    runs = {}
    page = 1
    expected_count = None

    # RETRIEVE EVERY PAGE OF RUNS FOR THE DATE FILTER
    while True:

        data, metadata = list_workflow_runs(
            per_page=100,
            page=page,
            created=created_filter,
        )

        # CAPTURE THE TOTAL COUNT ONCE, ON THE FIRST PAGE
        if expected_count is None:
            expected_count = data["total_count"]

            # REJECT WINDOWS THAT EXCEED GITHUB'S 1,000-RESULT CAP
            if expected_count >= 1000:
                raise RuntimeError(
                    "Too many runs for this date window. " "Use smaller windows."
                )

        batch = data["workflow_runs"]

        print(f"Page {page}: {len(batch)} runs")

        # KEY BY RUN ID SO OVERLAPPING PAGES CANNOT DUPLICATE
        for run in batch:
            runs[run["id"]] = run

        # STOP WHEN NO NEXT PAGE EXISTS
        if not metadata["has_next"]:
            break

        page += 1

    # SANITY CHECK: COLLECTED COUNT MUST MATCH GITHUB'S REPORT
    if len(runs) != expected_count:
        raise RuntimeError(
            f"Incomplete API results: "
            f"expected {expected_count}, "
            f"collected {len(runs)}"
        )

    # KEEP ONLY RUNS CREATED WITHIN THE LAST 24 HOURS
    recent = []

    for run in runs.values():
        created_at = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))

        if created_at >= window_start:
            recent.append(run)

    # SORT NEWEST FIRST
    recent.sort(
        key=lambda run: run["created_at"],
        reverse=True,
    )

    print("\n" + "=" * 60)
    print("INCREMENTAL PREVIEW SUMMARY")
    print("=" * 60)
    print(f"Unique runs in date filter: {len(runs)}")
    print(f"Runs created in last 24 hours: {len(recent)}")

    # SHOW AT MOST THE TEN MOST RECENT RUNS
    for run in recent[:10]:
        print(
            f"Run {run['id']} | "
            f"status={run['status']} | "
            f"conclusion={run['conclusion']} | "
            f"updated_at={run['updated_at']}"
        )


if __name__ == "__main__":
    main()
