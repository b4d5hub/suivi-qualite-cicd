import argparse
import time
from datetime import datetime, timedelta, timezone

from github_client import list_deployments, list_deployment_statuses
from kafka_producer import publish_messages
from state import (
    ensure_polling_table,
    ensure_watch_table,
    forget_watched,
    get_last_poll,
    load_watched_deployments,
    needs_publishing,
    open_state,
    record_successful_poll,
    remember_published,
    remember_watched,
    table_exists,
)
from transform import parse_time, transform_deployment

STREAM = "deployments"
LOOKBACK = timedelta(days=7)
OVERLAP = timedelta(minutes=15)

MAX_PAGES = 100
MAX_STATUS_CHECKS = 150

TERMINAL_STATES = {
    "success",
    "failure",
    "error",
    "inactive",
}


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


def discover_deployments(start, scan_start):
    """
    Scan deployment pages until records are older
    than the requested cutoff.

    Fail if the observed newest-first ordering changes.
    """
    discovered = {}
    seen_ids = set()
    previous_created = None

    for page in range(1, MAX_PAGES + 1):
        batch, metadata = list_deployments(
            per_page=100,
            page=page,
        )

        if not isinstance(batch, list):
            raise RuntimeError("Invalid GitHub deployments response")

        if not batch and metadata["has_next"]:
            raise RuntimeError("Empty page with additional pages reported")

        reached_cutoff = False

        for deployment in batch:
            deployment_id = deployment["id"]
            created_at = parse_time(deployment["created_at"])

            # Prevent unnoticed pagination overlap.
            if deployment_id in seen_ids:
                raise RuntimeError(f"Duplicate deployment ID: {deployment_id}")

            seen_ids.add(deployment_id)

            # We observed newest-first API ordering.
            # Abort rather than silently skip records
            # if the ordering is different.
            if previous_created is not None and created_at > previous_created:
                raise RuntimeError(
                    "Deployment ordering changed. " "Cannot safely stop at cutoff."
                )

            previous_created = created_at

            if created_at < start:
                reached_cutoff = True

            if start <= created_at <= scan_start:
                discovered[deployment_id] = deployment

        print(f"Page {page}: {len(batch)} deployments")

        if reached_cutoff or not metadata["has_next"]:
            return discovered

    raise RuntimeError(
        "Deployment page safety limit reached. " "Watermark was not advanced."
    )


def latest_status_state(statuses):
    if not statuses:
        return None

    latest = max(
        statuses,
        key=lambda status: (
            status["created_at"],
            status["id"],
        ),
    )

    return latest["state"]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Publish to Kafka and save checkpoints",
    )

    parser.add_argument(
        "--simulate-failure",
        action="store_true",
        help="Test failure recovery before saving watermark",
    )

    args = parser.parse_args()

    scan_start = datetime.now(timezone.utc)
    mode = "APPLY" if args.apply else "DRY RUN"

    print("=" * 60)
    print(f"INCREMENTAL DEPLOYMENTS -> KAFKA ({mode})")
    print("=" * 60)
    print(f"Scan start: {scan_start.isoformat()}")

    with open_state() as connection:

        if args.apply:
            ensure_polling_table(connection)
            ensure_watch_table(connection)
            connection.commit()

        if table_exists(connection, "polling_state"):
            previous = get_last_poll(connection, STREAM)
        else:
            previous = None

        discovery_start = scan_start - LOOKBACK

        if previous is not None:
            if previous >= scan_start:
                raise RuntimeError("Invalid previous deployment watermark")

            discovery_start = min(
                discovery_start,
                previous - OVERLAP,
            )

        print(f"Previous watermark: {previous}")
        print(f"Discovery starts: {discovery_start}")

        # Step 1: Find deployments in the scan window.
        discovered = discover_deployments(
            discovery_start,
            scan_start,
        )

        # Step 2: Include previously watched deployments,
        # even when they are older than the scan window.
        watched = load_watched_deployments(connection)

        candidates = dict(watched)
        candidates.update(discovered)

        print(f"Deployments discovered: {len(discovered)}")
        print(f"Previously watched: {len(watched)}")
        print(f"Unique candidates: {len(candidates)}")

        # Protect GitHub API rate limits.
        if len(candidates) > MAX_STATUS_CHECKS:
            raise RuntimeError(
                f"Too many deployments to check: "
                f"{len(candidates)} > {MAX_STATUS_CHECKS}. "
                "No watermark was advanced."
            )

        delivered = 0
        unchanged = 0

        ordered = sorted(
            candidates.values(),
            key=lambda deployment: (
                deployment["created_at"],
                deployment["id"],
            ),
        )

        watch_cutoff = scan_start - LOOKBACK

        for index, deployment in enumerate(ordered, 1):
            deployment_id = deployment["id"]

            # Step 3: Retrieve all available statuses.
            statuses = get_all_statuses(deployment_id)

            state = latest_status_state(statuses)

            # Step 4: Reuse historical transformation.
            record = transform_deployment(
                deployment,
                statuses,
            )

            message = (
                "deployments",
                str(deployment_id),
                record,
            )

            pending = needs_publishing(
                connection,
                message[0],
                message[1],
                message[2],
            )

            if pending:
                if args.apply:
                    # Publish first, checkpoint after ACK.
                    publish_messages([message])
                    remember_published(
                        connection,
                        [message],
                    )
                    connection.commit()

                delivered += 1
            else:
                unchanged += 1

            # Keep recent deployments and deployments
            # whose latest state is not terminal.
            keep_watching = (
                parse_time(deployment["created_at"]) >= watch_cutoff
                or state not in TERMINAL_STATES
            )

            if args.apply:
                if keep_watching:
                    remember_watched(
                        connection,
                        deployment,
                        state,
                        scan_start,
                    )
                else:
                    forget_watched(
                        connection,
                        deployment_id,
                    )

                connection.commit()

            print(
                f"[{index}/{len(ordered)}] "
                f"Deployment {deployment_id} | "
                f"state={state} | "
                f"{'new/changed' if pending else 'unchanged'}"
            )

            # Avoid a burst of status API calls.
            time.sleep(0.2)

        # Step 5: Advance the watermark only after
        # every requested API check and Kafka delivery.
        if args.simulate_failure:
            raise RuntimeError("SIMULATED FAILURE: watermark must not advance")

        if args.apply:
            record_successful_poll(
                connection,
                STREAM,
                scan_start,
            )
            connection.commit()

        print("\n" + "=" * 60)
        print("INCREMENTAL DEPLOYMENT SUMMARY")
        print("=" * 60)
        print(f"Mode: {mode}")
        print(f"Deployments discovered: {len(discovered)}")
        print(f"Previously watched: {len(watched)}")
        print(f"Unique candidates: {len(candidates)}")

        if args.apply:
            print(f"Messages delivered: {delivered}")
        else:
            print(f"Messages that would publish: {delivered}")

        print(f"Unchanged messages: {unchanged}")

        if args.apply:
            saved = get_last_poll(connection, STREAM)
            print(f"Saved watermark: {saved.isoformat()}")
        else:
            print("No Kafka or checkpoint changes.")

        print("Deployment scan completed successfully.")


if __name__ == "__main__":
    main()
