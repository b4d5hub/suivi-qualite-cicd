import _collector_path

import requests
from confluent_kafka import KafkaException
from github_client import (
    list_workflow_runs,
    list_workflow_run_jobs,
    list_deployments,
    list_deployment_statuses,
)
from transform import transform_build, transform_job, transform_deployment
from kafka_producer import publish_messages


def main():
    print("=" * 60)
    print("GITHUB API -> KAFKA INGESTION TEST")
    print("=" * 60)

    try:

        # STEP 1: GET A COMPLETED CI WORKFLOW
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
            raise RuntimeError("No completed CI run found.")

        # BUILD THE BUILDS MESSAGE FROM THE SELECTED RUN
        build_message = transform_build(run)

        # STEP 2: GET A JOB FROM THE SAME RUN
        jobs_data, _ = list_workflow_run_jobs(
            run_id=run["id"], attempt_number=run["run_attempt"], per_page=100
        )

        jobs = jobs_data.get("jobs", [])

        if not jobs:
            raise RuntimeError("No jobs found for selected run.")

        # BUILD THE TESTS MESSAGE FROM THE FIRST JOB
        job_message = transform_job(jobs[0], run)

        # STEP 3: GET A DEPLOYMENT
        deployments, _ = list_deployments(per_page=1)

        if not deployments:
            raise RuntimeError("No deployments found.")

        deployment = deployments[0]

        # COLLECT EVERY AVAILABLE STATUS PAGE
        statuses = []
        page = 1

        while True:

            batch, metadata = list_deployment_statuses(
                deployment_id=deployment["id"], per_page=100, page=page
            )

            # ACCUMULATE STATUSES FROM THE CURRENT PAGE
            statuses.extend(batch)

            # STOP WHEN NO NEXT PAGE EXISTS
            if not metadata["has_next"]:
                break

            page += 1

        # BUILD THE DEPLOYMENTS MESSAGE FROM THE DEPLOYMENT AND ITS STATUSES
        deployment_message = transform_deployment(deployment, statuses)

        # STEP 4: PREPARE ONE MESSAGE FOR EACH KAFKA TOPIC
        # Each entry is (topic, key, payload)
        messages = [
            ("builds", f"{run['id']}:{run['run_attempt']}", build_message),
            ("tests", str(jobs[0]["id"]), job_message),
            ("deployments", str(deployment["id"]), deployment_message),
        ]

        # STEP 5: SEND THE MESSAGES
        print("\nPublishing GitHub events to Kafka...")
        publish_messages(messages)

    except (requests.RequestException, KafkaException, RuntimeError) as error:

        print(f"\nIngestion test failed: {error}")


if __name__ == "__main__":
    main()
