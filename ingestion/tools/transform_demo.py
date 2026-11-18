import _collector_path

import json

from github_client import (
    list_workflow_runs,
    list_workflow_run_jobs,
    list_deployments,
    list_deployment_statuses,
)

from transform import transform_build, transform_job, transform_deployment


def main():

    print("JSON TRANSFORMATION TEST")

    # STEP 1: RETRIEVE A COMPLETED CI WORKFLOW
    data, _ = list_workflow_runs(per_page=30)

    run = next(
        (
            r
            for r in data["workflow_runs"]
            if r.get("name") == "CI - Node.js" and r.get("status") == "completed"
        ),
        None,
    )

    if run is None:
        print("No completed CI - Node.js run found.")
        return

    # BUILD THE BUILDS MESSAGE FROM THE SELECTED RUN
    build_message = transform_build(run)

    print("\nBUILDS MESSAGE")
    print(json.dumps(build_message, indent=2))

    # STEP 2: RETRIEVE JOBS FOR THAT RUN ATTEMPT
    jobs_data, _ = list_workflow_run_jobs(
        run_id=run["id"], attempt_number=run["run_attempt"], per_page=100
    )

    jobs = jobs_data.get("jobs", [])

    # BUILD THE TESTS MESSAGE FROM THE FIRST JOB
    if jobs:
        job_message = transform_job(jobs[0], run)

        print("\nTESTS MESSAGE")
        print(json.dumps(job_message, indent=2))

    # STEP 3: RETRIEVE A REAL DEPLOYMENT
    deployments, _ = list_deployments(per_page=1)

    if not deployments:
        print("No deployments found.")
        return

    deployment = deployments[0]

    # COLLECT EVERY AVAILABLE STATUS PAGE
    all_statuses = []
    page = 1

    while True:

        statuses, metadata = list_deployment_statuses(
            deployment_id=deployment["id"], per_page=100, page=page
        )

        # ACCUMULATE STATUSES FROM THE CURRENT PAGE
        all_statuses.extend(statuses)

        # STOP WHEN NO NEXT PAGE EXISTS
        if not metadata["has_next"]:
            break

        page += 1

    # BUILD THE DEPLOYMENTS MESSAGE FROM THE DEPLOYMENT AND ITS STATUSES
    deployment_message = transform_deployment(deployment, all_statuses)

    print("\nDEPLOYMENTS MESSAGE")
    print(json.dumps(deployment_message, indent=2))


if __name__ == "__main__":
    main()
