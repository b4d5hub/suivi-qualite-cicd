import argparse
from datetime import date

from backfill_deployments_preview import preview_deployments
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

    # STEP 1: RETRIEVE REAL GITHUB DEPLOYMENT DATA
    records, scan_complete = preview_deployments(
        args.date,
        args.max_pages,
    )

    # WARN IF THE SCAN WAS TRUNCATED BY max_pages
    if not scan_complete:
        print(
            "\nWARNING: Partial deployment scan. "
            "Publishing sample records only; "
            "the day is NOT marked complete."
        )

    # STEP 2: BUILD KAFKA MESSAGES (ONE PER DEPLOYMENT RECORD)
    messages = [
        (
            "deployments",
            str(record["deployment_id"]),
            record,
        )
        for record in records
    ]

    # OPEN THE LOCAL CHECKPOINT DATABASE
    connection = open_state()

    try:

        # STEP 3: KEEP ONLY NEW OR CHANGED MESSAGES
        pending = [
            (topic, key, payload)
            for topic, key, payload in messages
            if needs_publishing(
                connection,
                topic,
                key,
                payload,
            )
        ]

        skipped = len(messages) - len(pending)

        # STEP 4: PUBLISH AND WAIT FOR KAFKA CONFIRMATION
        if pending:
            publish_messages(pending)

            # STEP 5: SAVE CHECKPOINTS ONLY AFTER DELIVERY
            remember_published(connection, pending)

        print("\n" + "=" * 60)
        print("DEPLOYMENT KAFKA SUMMARY")
        print("=" * 60)

        print(f"Deployments prepared: {len(messages)}")
        print(f"Messages delivered: {len(pending)}")
        print(f"Unchanged messages skipped: {skipped}")
        print(f"Full list scanned: {scan_complete}")

    finally:
        # ALWAYS CLOSE THE CHECKPOINT CONNECTION
        connection.close()


if __name__ == "__main__":
    main()
