import _collector_path

from datetime import datetime, timedelta, timezone

from github_client import list_workflow_runs


def parse_github_time(value):

    # PARSE AN ISO 8601 TIMESTAMP (NORMALIZE "Z" TO "+00:00")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():

    now = datetime.now(timezone.utc)

    # CHANGE CUTOFF: THE COLLECTOR RUNS EVERY 5 MINUTES,
    # SO A 15-MINUTE WINDOW PROVIDES AN OVERLAP
    change_cutoff = now - timedelta(minutes=15)

    # DISCOVERY WINDOW: LOOK BACK 48 HOURS FOR RUNS
    discovery_start = now - timedelta(hours=48)

    # BUILD THE GITHUB DATE-RANGE FILTER (DATE-LEVEL GRANULARITY ONLY)
    created_filter = f"{discovery_start.date()}..{now.date()}"

    print("=" * 60)
    print("INCREMENTAL CHANGE DETECTION PREVIEW")
    print("=" * 60)
    print(f"Current UTC time: {now.isoformat()}")
    print(f"Change cutoff: {change_cutoff.isoformat()}")
    print(f"GitHub created filter: {created_filter}")

    # PAGINATION AND DEDUPLICATION STATE
    runs = {}
    expected_count = None
    page = 1

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
                    "Discovery window contains too many runs. "
                    "Use smaller date windows."
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
            f"Incomplete collection: expected "
            f"{expected_count}, received {len(runs)}"
        )

    # BUCKETS FOR EACH CHANGE CATEGORY
    newly_created = []
    recently_updated = []
    unfinished = []

    # CLASSIFY EVERY RUN INTO THE BUCKETS
    for run in runs.values():

        created_at = parse_github_time(run["created_at"])
        updated_at = parse_github_time(run["updated_at"])

        if created_at >= change_cutoff:
            newly_created.append(run)

        if updated_at >= change_cutoff:
            recently_updated.append(run)

        if run["status"] != "completed":
            unfinished.append(run)

    # MERGE NEW AND UPDATED INTO A UNIQUE CANDIDATE SET
    # (A RUN MAY BE BOTH NEW AND UPDATED)
    candidates = {run["id"]: run for run in newly_created + recently_updated}

    print("\n" + "=" * 60)
    print("INCREMENTAL CHANGE SUMMARY")
    print("=" * 60)

    print(f"Runs scanned: {len(runs)}")
    print(f"New runs in last 15 minutes: {len(newly_created)}")
    print(f"Updated runs in last 15 minutes: {len(recently_updated)}")
    print(f"Unfinished runs in discovery window: {len(unfinished)}")
    print(f"Unique change candidates: {len(candidates)}")

    # SHOW AT MOST THE TEN FIRST CANDIDATES
    for run in list(candidates.values())[:10]:
        print(
            f"Run {run['id']} | "
            f"status={run['status']} | "
            f"conclusion={run['conclusion']} | "
            f"updated_at={run['updated_at']}"
        )


if __name__ == "__main__":
    main()
