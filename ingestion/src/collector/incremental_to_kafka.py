import os
from datetime import datetime, timedelta, timezone

from github_client import (
    github_get,
    get_headers,
    list_workflow_runs,
)
from incremental_kafka_dry_run import parse_time
from backfill_to_kafka import prepare_run_messages
from kafka_producer import publish_messages
from state import (
    open_state,
    needs_publishing,
    remember_published,
)
from unfinished_runs import (
    ensure_unfinished_table,
    list_unfinished_run_ids,
    remember_unfinished,
    forget_unfinished,
)
from polling_state import (
    ensure_polling_table,
    get_last_poll,
    record_successful_poll,
)

STREAM = "workflows"
OVERLAP = timedelta(minutes=15)
LOOKBACK = timedelta(hours=48)


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
