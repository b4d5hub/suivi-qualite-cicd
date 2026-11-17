import requests

from github_client import list_workflow_runs, list_workflow_run_jobs


def main():

    print("=" * 60)
    print("GITHUB API - JOBS EXTRACTION")
    print("=" * 60)

    try:

        # STEP 1: RETRIEVE RECENT WORKFLOW RUNS
        data, _ = list_workflow_runs(per_page=30, page=1)

        runs = data.get("workflow_runs", [])

        # STEP 2: FIND A COMPLETED CI - NODE.JS RUN
        selected_run = next(
            (
                run
                for run in runs
                if run.get("name") == "CI - Node.js"
                and run.get("status") == "completed"
                and run.get("run_attempt")
            ),
            None,
        )

        if selected_run is None:
            print("No completed CI - Node.js run found.")
            return

        # STEP 3: EXTRACT THE RUN ID AND ATTEMPT
        run_id = selected_run["id"]
        attempt_number = selected_run["run_attempt"]

        print(f"\nWorkflow: {selected_run['name']}")
        print(f"Run ID: {run_id}")
        print(f"Attempt: {attempt_number}")
        print(f"Conclusion: {selected_run.get('conclusion')}")

        # STEP 4: RETRIEVE ALL JOBS FOR THIS ATTEMPT (PAGINATED)
        all_jobs = []
        page = 1

        while True:

            jobs_data, metadata = list_workflow_run_jobs(
                run_id=run_id, attempt_number=attempt_number, per_page=100, page=page
            )

            jobs = jobs_data.get("jobs", [])

            # ACCUMULATE JOBS FROM THE CURRENT PAGE
            all_jobs.extend(jobs)

            print(f"\nJobs received on page {page}: {len(jobs)}")
            print(f"Requests remaining: {metadata['remaining']}")

            # STOP WHEN NO JOBS RETURNED OR NO NEXT PAGE EXISTS
            if not jobs or not metadata["has_next"]:
                break

            page += 1

        # STEP 5: DISPLAY RESULTS
        print("\n" + "=" * 60)
        print("JOBS SUMMARY")
        print("=" * 60)

        print(f"Total jobs retrieved: {len(all_jobs)}")

        for job in all_jobs:

            print("\n" + "-" * 60)

            print(f"Job ID: {job.get('id')}")
            print(f"Job name: {job.get('name')}")
            print(f"Status: {job.get('status')}")
            print(f"Conclusion: {job.get('conclusion')}")
            print(f"Started: {job.get('started_at')}")
            print(f"Completed: {job.get('completed_at')}")

            # NUMBER OF STEPS RECORDED FOR THIS JOB
            steps = job.get("steps", [])

            print(f"Number of steps: {len(steps)}")

    except (requests.RequestException, RuntimeError) as error:

        print(f"\nGitHub API error: {error}")


if __name__ == "__main__":
    main()
