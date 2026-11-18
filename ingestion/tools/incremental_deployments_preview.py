import _collector_path

from datetime import datetime, timedelta, timezone

from github_client import list_deployments
from deployments import get_all_statuses


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    print("=" * 60)
    print("INCREMENTAL DEPLOYMENT DISCOVERY PREVIEW")
    print("=" * 60)
    print(f"Current UTC time: {now.isoformat()}")
    print(f"Recent cutoff: {cutoff.isoformat()}")

    deployments = {}
    pages_scanned = 0
    has_more_pages = False

    # Bounded API sample, not a complete repository scan.
    for page in range(1, 4):
        batch, metadata = list_deployments(
            per_page=50,
            page=page,
        )

        if not isinstance(batch, list):
            raise RuntimeError("Invalid deployments response")

        pages_scanned += 1

        for deployment in batch:
            deployments[deployment["id"]] = deployment

        times = [parse_time(item["created_at"]) for item in batch]

        print(f"Page {page}: {len(batch)} deployments")

        if times:
            print(f"  Oldest: {min(times).isoformat()}")
            print(f"  Newest: {max(times).isoformat()}")

        has_more_pages = metadata["has_next"]

        if not has_more_pages:
            break

    all_deployments = list(deployments.values())

    recent = [
        item for item in all_deployments if parse_time(item["created_at"]) >= cutoff
    ]

    recent.sort(
        key=lambda item: item["created_at"],
        reverse=True,
    )

    print("\n" + "=" * 60)
    print("DEPLOYMENT DISCOVERY SUMMARY")
    print("=" * 60)
    print(f"Pages scanned: {pages_scanned}")
    print(f"Unique deployments sampled: {len(deployments)}")
    print(f"Created in last 24 hours (sample): {len(recent)}")
    print(f"More pages available: {has_more_pages}")

    # Check a few real deployments and their full
    # status histories without publishing to Kafka.
    sample = recent[:3]

    if not sample:
        sample = sorted(
            all_deployments,
            key=lambda item: item["created_at"],
            reverse=True,
        )[:3]

    for deployment in sample:
        statuses = get_all_statuses(deployment["id"])

        latest = (
            max(
                statuses,
                key=lambda status: status["created_at"],
            )
            if statuses
            else None
        )

        print(f"\nDeployment {deployment['id']}")
        print(f"  Environment: {deployment['environment']}")
        print(f"  Created: {deployment['created_at']}")
        print(f"  Status records: {len(statuses)}")
        print(f"  Latest state: " f"{latest['state'] if latest else 'none'}")

    print("\nRead-only preview: no Kafka publication.")
    print("Coverage: sampled pages only, not complete.")


if __name__ == "__main__":
    main()
