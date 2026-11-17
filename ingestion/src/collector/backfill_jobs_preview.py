from datetime import datetime, timedelta, timezone

import requests

from backfill_preview import collect_one_day
from github_client import list_workflow_run_jobs
from transform import transform_job


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


def main():

    # SELECT A COMPLETE DAY, THREE DAYS BEFORE TODAY (UTC)
    target_date = datetime.now(timezone.utc).date() - timedelta(days=3)

    print("DAILY WORKFLOW AND JOB COLLECTION")

    try:

        # STEP 1: COLLECT ALL WORKFLOW RUNS FOR THE DAY
        runs = collect_one_day(target_date)

        # STEP 2: COLLECT JOBS FOR EVERY RUN
        all_jobs = []

        for index, run in enumerate(runs, start=1):

            if not jobs:
                print(
                    f"  No jobs: status={run.get('status')}, "
                    f"conclusion={run.get('conclusion')}, "
                    f"attempts={run.get('run_attempt')}"
                )

            print(f"\n[{index}/{len(runs)}] " f"Run {run['id']} | {run.get('name')}")

            jobs = collect_jobs_for_run(run)

            # ACCUMULATE MESSAGES FROM THIS RUN
            all_jobs.extend(jobs)

            print(f"Jobs collected: {len(jobs)}")

        # STEP 3: VALIDATE JOB UNIQUENESS
        job_ids = [job["job_id"] for job in all_jobs]

        unique_job_ids = set(job_ids)

        print("\n" + "=" * 60)
        print("DAILY JOB COLLECTION SUMMARY")
        print("=" * 60)

        print(f"Workflow runs: {len(runs)}")
        print(f"Jobs collected: {len(all_jobs)}")
        print(f"Unique job IDs: {len(unique_job_ids)}")

        if len(job_ids) != len(unique_job_ids):
            raise RuntimeError("Duplicate job IDs detected.")

        # STEP 4: COUNT JOB CONCLUSIONS
        conclusions = {}

        for job in all_jobs:

            conclusion = job.get("conclusion") or "none"

            conclusions[conclusion] = conclusions.get(conclusion, 0) + 1

        print("\nJob conclusions:")

        for conclusion, count in sorted(conclusions.items()):
            print(f"{conclusion}: {count}")

        print("\nValidation complete.")

    except (requests.RequestException, RuntimeError) as error:

        print(f"Collection failed: {error}")


if __name__ == "__main__":
    main()
