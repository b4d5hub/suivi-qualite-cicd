import requests

from github_client import list_workflow_runs


def main():
    max_pages = 3
    per_page = 10
    all_runs = []

    print("=" * 60)
    print("GITHUB API - PAGINATION TEST")
    print("=" * 60)

    try:
        for page in range(1, max_pages + 1):

            print(f"\nFetching page {page}...")

            data, metadata = list_workflow_runs(per_page, page)
            runs = data.get("workflow_runs", [])

            # ADD RETREIVED RUNS TO OUR LIST
            all_runs.extend(runs)
            print(f"Runs received:{len(runs)}")
            print("API requests remaining:", metadata["remaining"])

            if not runs:
                print("No more workflow runs")
                break

            if not metadata["has_next"]:
                print("Reached last page")
                break

        print("\n" + "=" * 60)
        print("COLLECTION SUMMARY")
        print("=" * 60)

        print(f"Total runs retrieved: {len(all_runs)}")

        # Check how many distinct run IDs we collected
        unique_ids = {run["id"] for run in all_runs}

        print(f"Unique run IDs: {len(unique_ids)}")

        print("\nRetrieved workflows:")

        for run in all_runs:
            print(f"{run['id']} | " f"{run.get('name')} | " f"{run.get('conclusion')}")

    except (requests.RequestException, RuntimeError) as error:
        print(f"Collection failed: {error}")


if __name__ == "__main__":
    main()
