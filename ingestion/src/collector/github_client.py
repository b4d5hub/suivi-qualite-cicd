import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# Find the repository root directory
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Load variables from the root .env file
load_dotenv(PROJECT_ROOT / ".env")


# GITHUB API CONFIG
API_BASE_URL = "https://api.github.com"

OWNER = os.getenv("GITHUB_OWNER", "freeCodeCamp")
REPO = os.getenv("GITHUB_REPO", "freeCodeCamp")


def get_headers():

    # GET TOKEN FROM ENV
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        raise RuntimeError("GITHUB_TOKEN is missing. Check your .env file.")

    # HTTP REQUEST HEADERS
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2026-03-10",
        "User-Agent": "suivi-qualite-cicd",
    }


def get_rate_limit(response):

    # EXTRACT RATE-LIMIT INFORMATION FROM RESPONSE HEADERS
    return {
        "limit": response.headers.get("X-RateLimit-Limit"),
        "remaining": response.headers.get("X-RateLimit-Remaining"),
        "reset": response.headers.get("X-RateLimit-Reset"),
        "has_next": "next" in response.links
    }


def list_workflow_runs(per_page=5, page=1):

    # API ENDPOINT
    url = f"{API_BASE_URL}/repos/{OWNER}/{REPO}/actions/runs"

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=30
    )
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)


def list_workflow_run_jobs(run_id, attempt_number, per_page=100, page=1):

    # API ENDPOINT FOR A SPECIFIC WORKFLOW RUN ATTEMPT
    url = (
        f"{API_BASE_URL}/repos/{OWNER}/{REPO}/actions/"
        f"runs/{run_id}/attempts/{attempt_number}/jobs"
    )

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=30
    )
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)


def list_deployments(per_page=5, page=1):

    # API ENDPOINT
    url = f"{API_BASE_URL}/repos/{OWNER}/{REPO}/deployments"

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=30
    )
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)


def list_deployment_statuses(deployment_id, per_page=10, page=1):

    # API ENDPOINT FOR ONE DEPLOYMENT'S STATUSES
    url = (
        f"{API_BASE_URL}/repos/{OWNER}/{REPO}/"
        f"deployments/{deployment_id}/statuses"
    )

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=30
    )
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)