import _collector_path

from datetime import datetime, timedelta, timezone

from github_client import list_workflow_runs
from state import needs_publishing, open_state
from transform import parse_time
from workflows import prepare_run_messages


def collect_recent_runs(now):

    # BUILD THE 48-HOUR DISCOVERY WINDOW
    start = now - timedelta(hours=48)
    created_filter = f"{start.date()}..{now.date()}"

    # PAGINATION AND DEDUPLICATION STATE
    page = 1
    expected_count = None
    runs = {}

    # RETRIEVE EVERY PAGE OF RUNS FOR THE DATE FILTER
    while True:

        data, metadata = list_workflow_runs(
            per_page=100,
            page=page,
            created=created_filter,
        )

        count = data.get("total_count")

        # REJECT MALFORMED COUNTS FROM THE API
        if not isinstance(count, int):
            raise RuntimeError("Invalid GitHub total_count")

        if expected_count is None:
            expected_count = count

            # REJECT WINDOWS THAT EXCEED GITHUB'S 1,000-RESULT CAP
            if expected_count >= 1000:
                raise RuntimeError("Too many runs. Split the discovery window.")
        elif count != expected_count:
            # THE TOTAL COUNT MUST NOT DRIFT BETWEEN PAGES
            raise RuntimeError("GitHub total_count changed during pagination")

        batch = data.get("workflow_runs", [])

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
            f"Incomplete collection: expected {expected_count}, "
            f"received {len(runs)}"
        )

    return list(runs.values())


def main():

    # CHANGE CUTOFF: THE COLLECTOR RUNS EVERY 5 MINUTES,
    # SO A 15-MINUTE WINDOW PROVIDES AN OVERLAP
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=15)

    print("=" * 60)
    print("INCREMENTAL KAFKA DRY RUN")
    print("=" * 60)
    print(f"UTC time: {now.isoformat()}")
    print(f"Change cutoff: {cutoff.isoformat()}")

    # COLLECT EVERY RUN FROM THE DISCOVERY WINDOW
    runs = collect_recent_runs(now)

    # SELECT RUNS THAT ARE NEW, RECENTLY UPDATED, OR UNFINISHED
    candidates = []

    for run in runs:

        created_at = parse_time(run["created_at"])
        updated_at = parse_time(run["updated_at"])

        if created_at >= cutoff or updated_at >= cutoff or run["status"] != "completed":
            candidates.append(run)

    print(f"\nRuns scanned: {len(runs)}")
    print(f"Candidate runs: {len(candidates)}")

    # DRY-RUN COUNTERS
    prepared_count = 0
    pending_count = 0

    # OPEN THE CHECKPOINT DB AND EVALUATE EACH CANDIDATE
    with open_state() as connection:

        for run in candidates:

            # BUILD THE FULL MESSAGE SET FOR THIS RUN
            messages = prepare_run_messages(run)

            # KEEP ONLY MESSAGES THAT WOULD ACTUALLY PUBLISH
            pending = [
                message
                for message in messages
                if needs_publishing(
                    connection,
                    message[0],
                    message[1],
                    message[2],
                )
            ]

            prepared_count += len(messages)
            pending_count += len(pending)

            print(
                f"Run {run['id']} | "
                f"status={run['status']} | "
                f"prepared={len(messages)} | "
                f"would_publish={len(pending)}"
            )

    print("\n" + "=" * 60)
    print("KAFKA DRY RUN SUMMARY")
    print("=" * 60)
    print(f"Runs scanned: {len(runs)}")
    print(f"Candidate runs: {len(candidates)}")
    print(f"Messages prepared: {prepared_count}")
    print(f"Messages that would be published: {pending_count}")
    print(f"Unchanged messages: {prepared_count - pending_count}")
    print("Dry run only: no messages published.")


if __name__ == "__main__":
    main()
