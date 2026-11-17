import argparse
from datetime import date

from backfill_preview import collect_one_day
from backfill_jobs_preview import collect_jobs_for_run

from github_client import get_workflow_run_attempt
from transform import transform_build
from kafka_producer import publish_messages

from state import open_state, needs_publishing, remember_published


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


def main():

    # PARSE THE REQUIRED TARGET DATE FROM THE COMMAND LINE
    parser = argparse.ArgumentParser()

    parser.add_argument("--date", required=True, help="UTC date in YYYY-MM-DD format")

    args = parser.parse_args()
    target_date = date.fromisoformat(args.date)

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


if __name__ == "__main__":
    main()
