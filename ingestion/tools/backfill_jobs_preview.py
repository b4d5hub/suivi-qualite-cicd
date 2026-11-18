import _collector_path

from datetime import datetime, timedelta, timezone

import requests

from workflows import collect_jobs_for_run, collect_one_day


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
