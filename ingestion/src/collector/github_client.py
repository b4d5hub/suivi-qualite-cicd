import os, requests, time
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Find the repository root directory
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Load variables from the root .env file
load_dotenv(PROJECT_ROOT / ".env")


# GITHUB API CONFIG
API_BASE_URL = "https://api.github.com"

OWNER = os.getenv("GITHUB_OWNER", "freeCodeCamp")
REPO = os.getenv("GITHUB_REPO", "freeCodeCamp")


# Retry temporary GitHub API failures.
retry_policy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods={"GET"},
    respect_retry_after_header=True,
    raise_on_status=False,
)
SESSION = requests.Session()
SESSION.mount("https://api.github.com/", HTTPAdapter(max_retries=retry_policy))


def github_get(url, **kwargs):

    # MAXIMUM NUMBER OF RETRIES BEFORE GIVING UP
    max_retries = 3

    # RETRY LOOP (INCLUDES THE INITIAL ATTEMPT)
    for attempt in range(max_retries + 1):

        # SEND THE GET REQUEST
        try:
            response = SESSION.get(url, **kwargs)

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:

            if attempt == max_retries:
                raise

            wait_seconds = 5 * (2**attempt)

            print(
                f"Network error: {type(error).__name__}. "
                f"Retrying in {wait_seconds}s "
                f"({attempt + 1}/{max_retries})."
            )

            time.sleep(wait_seconds)
            continue

        # NON-RATE-LIMIT RESPONSES ARE RETURNED IMMEDIATELY
        if response.status_code not in (403, 429):
            return response

        # READ RATE-LIMIT HEADERS USED TO DECIDE THE WAIT TIME
        remaining = response.headers.get("X-RateLimit-Remaining")

        reset = response.headers.get("X-RateLimit-Reset")

        retry_after = response.headers.get("Retry-After")

        error_message = response.text.lower()

        # DISTINGUISH TRUE RATE LIMITS FROM ORDINARY 403 ERRORS
        is_rate_limited = (
            response.status_code == 429
            or remaining == "0"
            or "rate limit" in error_message
        )

        # A REGULAR 403 (E.G. MISSING PERMISSIONS)
        # SHOULD NOT BE RETRIED
        if not is_rate_limited:
            return response

        # LAST ATTEMPT: RETURN THE RESPONSE AS-IS
        if attempt == max_retries:
            return response

        # COMPUTE WAIT TIME: HONOR Retry-After FIRST
        if retry_after is not None:
            try:
                wait_seconds = max(1, float(retry_after))
            except ValueError:
                wait_seconds = 60 * (2**attempt)

        # THEN FALL BACK TO THE RESET TIMESTAMP
        elif remaining == "0" and reset:
            wait_seconds = max(1, int(reset) - time.time() + 2)

        else:
            # SECONDARY RATE LIMIT WITHOUT A WAIT HEADER: EXPONENTIAL BACKOFF
            wait_seconds = 60 * (2**attempt)

        print(
            f"GitHub rate limit reached. "
            f"Waiting {wait_seconds:.0f}s "
            f"before retry {attempt + 1}/{max_retries}."
        )

        # SLEEP BEFORE THE NEXT RETRY
        time.sleep(wait_seconds)

    return response


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
        "has_next": "next" in response.links,
    }


def list_workflow_runs(per_page=5, page=1, created=None):

    # API ENDPOINT
    url = f"{API_BASE_URL}/repos/{OWNER}/{REPO}/actions/runs"

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # APPLY A DATE FILTER ONLY WHEN ONE IS SUPPLIED
    if created is not None:
        params["created"] = created

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = github_get(url, headers=get_headers(), params=params, timeout=30)
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
    response = github_get(url, headers=get_headers(), params=params, timeout=30)
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)


def list_deployments(per_page=5, page=1):

    # API ENDPOINT
    url = f"{API_BASE_URL}/repos/{OWNER}/{REPO}/deployments"

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = github_get(url, headers=get_headers(), params=params, timeout=30)
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)


def list_deployment_statuses(deployment_id, per_page=10, page=1):

    # API ENDPOINT FOR ONE DEPLOYMENT'S STATUSES
    url = (
        f"{API_BASE_URL}/repos/{OWNER}/{REPO}/" f"deployments/{deployment_id}/statuses"
    )

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": page}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = github_get(url, headers=get_headers(), params=params, timeout=30)
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)


def get_workflow_run_attempt(run_id, attempt_number):

    # API ENDPOINT FOR A SPECIFIC WORKFLOW RUN ATTEMPT
    url = (
        f"{API_BASE_URL}/repos/{OWNER}/{REPO}/actions/"
        f"runs/{run_id}/attempts/{attempt_number}"
    )

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = github_get(url, headers=get_headers(), timeout=30)
    response.raise_for_status()

    # CONVERT JSON RESPONSE TO A PY DICTIONARY AND RETURN RATE-LIMIT INFO
    return response.json(), get_rate_limit(response)
