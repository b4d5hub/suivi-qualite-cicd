import _collector_path

import requests

from github_client import list_deployments, list_deployment_statuses


def main():

    print("=" * 60)
    print("GITHUB API - DEPLOYMENTS EXTRACTION")
    print("=" * 60)

    try:

        # STEP 1: RETRIEVE FIVE DEPLOYMENTS
        deployments, metadata = list_deployments(per_page=5, page=1)

        print(f"\nDeployments retrieved: {len(deployments)}")
        print(f"Requests remaining: {metadata['remaining']}")

        if not deployments:
            print("No deployments found in this response.")
            return

        # STEP 2: INSPECT EACH DEPLOYMENT
        for deployment in deployments:

            # DEPLOYMENT IDENTIFIER AND METADATA
            deployment_id = deployment["id"]

            print("\n" + "-" * 60)
            print(f"Deployment ID: {deployment_id}")
            print(f"Environment: {deployment.get('environment')}")
            print(f"Reference: {deployment.get('ref')}")
            print(f"Commit SHA: {deployment.get('sha')}")
            print(f"Created at: {deployment.get('created_at')}")

            # STEP 3: RETRIEVE THE FIRST PAGE OF STATUSES
            statuses, status_metadata = list_deployment_statuses(
                deployment_id=deployment_id, per_page=10, page=1
            )

            print(f"Statuses retrieved: {len(statuses)}")

            if not statuses:
                print("No deployment statuses available.")

            # DISPLAY EACH STATUS RETURNED FOR THIS DEPLOYMENT
            for status in statuses:
                print(
                    f"  Status ID: {status.get('id')} | "
                    f"State: {status.get('state')} | "
                    f"Created: {status.get('created_at')}"
                )

            # FLAG WHETHER MORE STATUS PAGES EXIST
            if status_metadata["has_next"]:
                print("More status pages are available.")

        print("\n" + "=" * 60)
        print("EXTRACTION COMPLETE")
        print("=" * 60)

    except (requests.RequestException, RuntimeError) as error:

        print(f"GitHub API error: {error}")


if __name__ == "__main__":
    main()
