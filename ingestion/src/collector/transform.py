from datetime import datetime


def calculate_duration(started_at, completed_at):

    # RETURN NONE IF EITHER TIMESTAMP IS MISSING
    if not started_at or not completed_at:
        return None

    # PARSE ISO 8601 TIMESTAMPS (NORMALIZE "Z" TO "+00:00")
    start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))

    end = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))

    # COMPUTE DURATION IN SECONDS
    duration = (end - start).total_seconds()

    # REJECT NEGATIVE DURATIONS (CLOCK SKEW / BAD DATA)
    if duration < 0:
        return None

    return int(duration)


def transform_build(run):

    # MAP A WORKFLOW RUN TO A BUILDS MESSAGE
    return {
        "run_id": run["id"],
        "workflow": run.get("name"),
        "run_attempt": run.get("run_attempt", 1),
        "event": run.get("event"),
        "branch": run.get("head_branch"),
        "head_sha": run.get("head_sha"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "created_at": run.get("created_at"),
        "started_at": run.get("run_started_at"),
        "updated_at": run.get("updated_at"),
        "duration_s": None,
    }


def transform_job(job, run, attempt_number=None):

    # RAW JOB TIMESTAMPS
    started_at = job.get("started_at")
    completed_at = job.get("completed_at")

    duration = None

    # ONLY MEASURE DURATION FOR COMPLETED, NON-SKIPPED JOBS
    if job.get("status") == "completed" and job.get("conclusion") != "skipped":
        duration = calculate_duration(started_at, completed_at)

    # MAP A GITHUB JOB TO A TESTS MESSAGE
    return {
        "job_id": job["id"],
        "run_id": run["id"],
        "run_attempt": (
            attempt_number if attempt_number is not None else run.get("run_attempt", 1)
        ),
        "head_sha": run.get("head_sha"),
        "job_name": job.get("name"),
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_s": duration,
    }


def transform_deployment(deployment, statuses):

    # SORT STATUS RECORDS CHRONOLOGICALLY
    ordered = sorted(
        statuses, key=lambda s: (s.get("created_at") or "", s.get("id") or 0)
    )

    # LATEST STATUS IS THE LAST ENTRY AFTER SORTING
    latest = ordered[-1] if ordered else None

    # BUILD THE FULL STATUS HISTORY LIST
    status_history = [
        {
            "status_id": status.get("id"),
            "state": status.get("state"),
            "created_at": status.get("created_at"),
        }
        for status in ordered
    ]

    # MAP A DEPLOYMENT AND ITS STATUSES TO ONE MESSAGE
    return {
        "deployment_id": deployment["id"],
        "environment": deployment.get("environment"),
        "production_environment": deployment.get("production_environment"),
        "ref": deployment.get("ref"),
        "sha": deployment.get("sha"),
        "created_at": deployment.get("created_at"),
        "updated_at": deployment.get("updated_at"),
        "latest_status": (latest.get("state") if latest else None),
        "latest_status_at": (latest.get("created_at") if latest else None),
        "status_history": status_history,
    }
