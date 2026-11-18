import os
from datetime import datetime, timedelta, timezone

from github_client import (
    get_headers,
    get_workflow_run_attempt,
    github_get,
    list_workflow_run_jobs,
    list_workflow_runs,
)
from kafka_producer import publish_messages
from state import (
    ensure_polling_table,
    ensure_unfinished_table,
    forget_unfinished,
    get_last_poll,
    list_unfinished_run_ids,
    needs_publishing,
    open_state,
    record_successful_poll,
    remember_published,
    remember_unfinished,
)
from transform import parse_time, transform_build, transform_job

STREAM = "workflows"
OVERLAP = timedelta(minutes=15)
LOOKBACK = timedelta(hours=48)


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


def collect_jobs_for_run(run):

    # ACCUMULATED TESTS MESSAGES FOR EVERY ATTEMPT
    all_jobs = []

    run_id = run["id"]
    latest_attempt = run.get("run_attempt", 1)

    # ITERATE OVER EVERY ATTEMPT OF THE RUN
    for attempt in range(1, latest_attempt + 1):

        page = 1

        # RETRIEVE PAGES OF JOBS FOR THE CURRENT ATTEMPT
        while True:

            data, metadata = list_workflow_run_jobs(
                run_id=run_id, attempt_number=attempt, per_page=100, page=page
            )

            jobs = data.get("jobs", [])

            # TRANSFORM EACH JOB INTO A TESTS MESSAGE
            for job in jobs:
                message = transform_job(job, run, attempt_number=attempt)

                all_jobs.append(message)

            # STOP WHEN NO NEXT PAGE EXISTS
            if not metadata["has_next"]:
                break

            page += 1

    return all_jobs


def prepare_run_messages(run):

    # ACCUMULATED MESSAGES FOR THIS RUN (BUILDS + TESTS)
    messages = []

    run_id = run["id"]
    latest_attempt = run.get("run_attempt") or 1

    # BUILD MESSAGES FOR EVERY ATTEMPT
    for attempt in range(1, latest_attempt + 1):

        # THE LATEST ATTEMPT IS ALREADY IN THE RUN PAYLOAD
        if attempt == latest_attempt:
            attempt_run = run
        else:
            # OLDER ATTEMPTS NEED A SEPARATE API CALL
            attempt_run, _ = get_workflow_run_attempt(run_id, attempt)

        build = transform_build(attempt_run)
        build["run_attempt"] = attempt

        messages.append(("builds", f"{run_id}:{attempt}", build))

    # JOB MESSAGES FOR EVERY ATTEMPT
    jobs = collect_jobs_for_run(run)

    for job in jobs:
        messages.append(("tests", str(job["job_id"]), job))

    return messages


def backfill_day(target_date):
    print("=" * 60)
    print("CHECKPOINTED GITHUB -> KAFKA BACKFILL")
    print("=" * 60)

    # COLLECT ALL WORKFLOW RUNS FOR THE TARGET DAY
    runs = collect_one_day(target_date)

    # OPEN THE LOCAL CHECKPOINT DATABASE
    connection = open_state()

    delivered_count = 0
    skipped_count = 0

    try:

        for index, run in enumerate(runs, start=1):

            print(f"\n[{index}/{len(runs)}] " f"Run {run['id']}")

            messages = prepare_run_messages(run)

            # KEEP ONLY NEW OR CHANGED MESSAGES
            pending = [
                (topic, key, payload)
                for topic, key, payload in messages
                if needs_publishing(connection, topic, key, payload)
            ]

            skipped = len(messages) - len(pending)
            skipped_count += skipped

            if not pending:
                print("No changes. Skipping.")
                continue

            # PUBLISH AND REQUIRE DELIVERY CONFIRMATION
            publish_messages(pending)

            # SAVE CHECKPOINTS ONLY AFTER SUCCESSFUL DELIVERY
            remember_published(connection, pending)

            delivered_count += len(pending)

            print(f"Published: {len(pending)} | " f"Unchanged: {skipped}")

        print("\n" + "=" * 60)
        print("BACKFILL SUMMARY")
        print("=" * 60)

        print(f"Workflow runs: {len(runs)}")
        print(f"Messages delivered: {delivered_count}")
        print(f"Unchanged messages skipped: {skipped_count}")

    finally:
        # ALWAYS CLOSE THE CHECKPOINT CONNECTION
        connection.close()


def fetch_run_by_id(run_id):
    owner = os.environ["GITHUB_OWNER"]
    repo = os.environ["GITHUB_REPO"]

    url = f"https://api.github.com/repos/{owner}/{repo}" f"/actions/runs/{run_id}"

    response = github_get(
        url,
        headers=get_headers(),
        timeout=30,
    )
    response.raise_for_status()

    run = response.json()

    if run["id"] != run_id:
        raise RuntimeError("Wrong GitHub run ID")

    return run


def collect_runs_between(start, end):
    """
    Discover runs by UTC creation date.
    Validate every day's pagination before continuing.
    """
    current_day = start.date()
    last_day = end.date()
    all_runs = {}

    while current_day <= last_day:
        day_text = current_day.isoformat()

        page = 1
        expected = None
        day_runs = {}

        while True:
            data, metadata = list_workflow_runs(
                per_page=100,
                page=page,
                created=f"{day_text}..{day_text}",
            )

            reported = data.get("total_count")

            if not isinstance(reported, int) or reported < 0:
                raise RuntimeError(f"{day_text}: invalid total_count")

            if reported >= 1000:
                raise RuntimeError(
                    f"{day_text}: split this day into smaller "
                    "time windows before continuing"
                )

            if expected is None:
                expected = reported
            elif reported != expected:
                raise RuntimeError(
                    f"{day_text}: total_count changed " "during pagination"
                )

            batch = data.get("workflow_runs")

            if not isinstance(batch, list):
                raise RuntimeError(f"{day_text}: invalid workflow_runs")

            for run in batch:
                created_at = parse_time(run["created_at"])

                if created_at.date() != current_day:
                    raise RuntimeError(
                        f"{day_text}: GitHub returned " "a run from another day"
                    )

                day_runs[run["id"]] = run

            if not metadata["has_next"]:
                break

            page += 1

            if page > 10:
                raise RuntimeError(f"{day_text}: unexpected pagination")

        if len(day_runs) != expected:
            raise RuntimeError(
                f"{day_text}: expected {expected}, " f"received {len(day_runs)}"
            )

        print(
            f"{day_text}: {len(day_runs)} " "runs collected",
            flush=True,
        )

        for run in day_runs.values():
            created_at = parse_time(run["created_at"])

            if start <= created_at <= end:
                all_runs[run["id"]] = run

        current_day += timedelta(days=1)

    return list(all_runs.values())


def main():
    scan_start = datetime.now(timezone.utc)

    print("=" * 60)
    print("INCREMENTAL GITHUB -> KAFKA WITH WATERMARK")
    print("=" * 60)

    with open_state() as connection:
        ensure_polling_table(connection)
        ensure_unfinished_table(connection)
        connection.commit()

        previous = get_last_poll(
            connection,
            STREAM,
        )

        if previous is None:
            # First execution: establish a 48-hour baseline.
            discovery_start = scan_start - LOOKBACK
            change_cutoff = discovery_start
        else:
            if scan_start <= previous:
                raise RuntimeError("System clock did not advance")

            # Always rescan at least 48 hours.
            # Extend the window after downtime.
            discovery_start = min(
                scan_start - LOOKBACK,
                previous - OVERLAP,
            )
            change_cutoff = previous - OVERLAP

        print(f"Previous watermark: {previous}")
        print(f"Discovery starts: {discovery_start}")
        print(f"Change cutoff: {change_cutoff}")

        # Remember IDs already tracked before this scan.
        tracked_before = list_unfinished_run_ids(connection)

        # GitHub discovery must succeed completely.
        runs = collect_runs_between(
            discovery_start,
            scan_start,
        )

        # Save newly discovered unfinished IDs.
        for run in runs:
            if run["status"] != "completed":
                remember_unfinished(connection, run)

        connection.commit()

        candidates = {}

        for run in runs:
            created_at = parse_time(run["created_at"])
            updated_at = parse_time(run["updated_at"])

            if (
                created_at >= change_cutoff
                or updated_at >= change_cutoff
                or run["status"] != "completed"
            ):
                candidates[run["id"]] = run

        # Recover older tracked workflows by direct ID.
        for run_id in tracked_before:
            print(f"Rechecking run {run_id}")

            latest = fetch_run_by_id(run_id)
            candidates[run_id] = latest

        ordered = sorted(
            candidates.values(),
            key=lambda run: (
                run["created_at"],
                run["id"],
            ),
        )

        print(f"Discovered runs: {len(runs)}")
        print(f"Unique candidates: {len(ordered)}")

        delivered = 0
        unchanged = 0

        for index, run in enumerate(ordered, 1):
            print(f"[{index}/{len(ordered)}] " f"Run {run['id']} | {run['status']}")

            messages = prepare_run_messages(run)

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

            if pending:
                # Publish and verify delivery first.
                publish_messages(pending)

                # Checkpoint only after Kafka delivery.
                remember_published(connection, pending)
                connection.commit()

            if run["status"] == "completed":
                forget_unfinished(connection, run["id"])
            else:
                remember_unfinished(connection, run)

            connection.commit()

            delivered += len(pending)
            unchanged += len(messages) - len(pending)

        # The watermark advances ONLY after all
        # API calls and required publications succeed.
        record_successful_poll(
            connection,
            STREAM,
            scan_start,
        )
        connection.commit()

        remaining = list_unfinished_run_ids(connection)
        saved_watermark = get_last_poll(
            connection,
            STREAM,
        )

    print("\n" + "=" * 60)
    print("INCREMENTAL WATERMARK SUMMARY")
    print("=" * 60)
    print(f"Runs discovered: {len(runs)}")
    print(f"Candidates processed: {len(ordered)}")
    print(f"Messages delivered: {delivered}")
    print(f"Unchanged messages skipped: {unchanged}")
    print(f"Unfinished runs tracked: {len(remaining)}")
    print(f"Saved watermark: {saved_watermark.isoformat()}")
    print("Incremental collection completed successfully.")


if __name__ == "__main__":
    main()
