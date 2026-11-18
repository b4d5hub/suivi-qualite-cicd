import _collector_path

import requests

from github_client import list_workflow_runs


def main():
    print("=" * 60)
    print("CI/CD QUALITY MONITORING")
    print("GitHub API Collector")
    print("=" * 60)

    try:

        # RETREIVE WORKFLOW RUNS (run the http request)
        data, rate_limit = list_workflow_runs(per_page=5)
        print(f"\nAPI rate limit: {rate_limit['limit']}")
        print(f"Requests remaining: {rate_limit['remaining']}")

        # EXTRACT WORKFLOW RUNS
        runs = data.get("workflow_runs", [])

        print(f"\nTotal matching runs: {data.get('total_count', 0)}")
        print(f"Runs retrieved in this request: {len(runs)}")

        print("\nWORKFLOW RUNS")
        print("-" * 60)

        for run in runs:
            print(f"ID: {run.get('id')}")
            print(f"Workflow: {run.get('name')}")
            print(f"Status: {run.get('status')}")
            print(f"Conclusion: {run.get('conclusion')}")
            print(f"Branch: {run.get('head_branch')}")
            print("-" * 60)

    except (requests.RequestException, RuntimeError) as error:

        print("\nGitHub API request failed.")
        print(f"Error: {error}")


if __name__ == "__main__":
    main()
