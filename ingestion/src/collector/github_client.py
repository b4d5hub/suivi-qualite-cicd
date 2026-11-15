import requests,os
from pathlib import Path
from dotenv import load_dotenv


# Find the repository root directory
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Load variables from the root .env file
load_dotenv(PROJECT_ROOT / ".env")


# GITHUB API CONFIG
API_BASE_URL = "https://api.github.com"

OWNER = os.getenv("GITHUB_OWNER", "freeCodeCamp")
REPO = os.getenv("GITHUB_REPO", "freeCodeCamp")


def list_workflow_runs(per_page=5):

    #GET TOKEN FROM ENV
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        raise RuntimeError("GITHUB_TOKEN is missing. Check your .env file.")


    # API ENDPOINT
    url = f"{API_BASE_URL}/repos/{OWNER}/{REPO}/actions/runs"

    # HTTP REQUEST HEADERS
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2026-03-10",
        "User-Agent": "suivi-qualite-cicd",
    }

    # REQUEST PARAMETERS
    params = {"per_page": per_page, "page": 1}

    # SEND THE GET REQUEST AND RAISE ERROR IF GITHUB RETURNS AN HTTP ERROR
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    # Extract rate-limit information
    rate_limit = {
        "limit": response.headers.get("X-RateLimit-Limit"),
        "remaining": response.headers.get("X-RateLimit-Remaining"),
        "reset": response.headers.get("X-RateLimit-Reset")
    }

    # CONVERT JSON RESPONSE TO A PY DICTIONARY
    return response.json(), rate_limit
