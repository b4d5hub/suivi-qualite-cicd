#!/usr/bin/env bash
set -euo pipefail

# Run the Lakehouse Bronze -> Silver pipeline and verification inside the spark container.
# Usage:
#   /opt/lakehouse/run_all.sh
#   /opt/lakehouse/run_all.sh --skip-verify
#   /opt/lakehouse/run_all.sh --only <step_name>

SKIP_VERIFY=false
ONLY_STEP=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-verify)
            SKIP_VERIFY=true
            shift
            ;;
        --only)
            if [[ $# -lt 2 || -z "${2:-}" ]]; then
                echo "[ERROR] --only requires a <step_name> argument." >&2
                exit 2
            fi
            ONLY_STEP="$2"
            shift 2
            ;;
        *)
            echo "[ERROR] Unknown argument: $1" >&2
            echo "Usage: /opt/lakehouse/run_all.sh [--skip-verify] [--only <step_name>]" >&2
            exit 2
            ;;
    esac
done

VALID_STEPS=(
    "bronze_ingest"
    "silver_builds"
    "silver_tests"
    "silver_deployments"
    "verify_bronze"
    "verify_silver"
    "verify_silver_builds"
    "verify_silver_tests"
    "verify_silver_deployments"
)

is_valid_step() {
    local target="$1"
    for s in "${VALID_STEPS[@]}"; do
        if [[ "$s" == "$target" ]]; then
            return 0
        fi
    done
    return 1
}

if [[ -n "$ONLY_STEP" ]]; then
    if ! is_valid_step "$ONLY_STEP"; then
        echo "[ERROR] Unknown step name: '${ONLY_STEP}'." >&2
        echo "[ERROR] Valid steps are: ${VALID_STEPS[*]}" >&2
        exit 2
    fi
    STEPS_TO_RUN=("$ONLY_STEP")
else
    STEPS_TO_RUN=(
        "bronze_ingest"
        "silver_builds"
        "silver_tests"
        "silver_deployments"
    )
    if [[ "$SKIP_VERIFY" == "false" ]]; then
        STEPS_TO_RUN+=(
            "verify_bronze"
            "verify_silver"
        )
    fi
fi

declare -a SUMMARY_LINES=()
PIPELINE_START=$SECONDS

run_step() {
    local step="$1"
    local step_start=$SECONDS
    local rc=0

    echo ""
    echo "============================================================"
    echo "[RUN_ALL] STARTING STEP: ${step}"
    echo "============================================================"

    set +e
    case "$step" in
        bronze_ingest)
            /opt/lakehouse/submit.sh src/bronze_ingest.py
            rc=$?
            ;;
        silver_builds)
            /opt/lakehouse/submit.sh src/silver_builds.py
            rc=$?
            ;;
        silver_tests)
            /opt/lakehouse/submit.sh src/silver_tests.py
            rc=$?
            ;;
        silver_deployments)
            /opt/lakehouse/submit.sh src/silver_deployments.py
            rc=$?
            ;;
        verify_bronze)
            /opt/lakehouse/submit.sh tools/verify_bronze.py
            rc=$?
            ;;
        verify_silver)
            /opt/lakehouse/submit.sh tools/verify_silver.py silver_builds silver_tests silver_deployments
            rc=$?
            ;;
        verify_silver_builds)
            /opt/lakehouse/submit.sh tools/verify_silver.py silver_builds
            rc=$?
            ;;
        verify_silver_tests)
            /opt/lakehouse/submit.sh tools/verify_silver.py silver_tests
            rc=$?
            ;;
        verify_silver_deployments)
            /opt/lakehouse/submit.sh tools/verify_silver.py silver_deployments
            rc=$?
            ;;
    esac
    set -e

    local elapsed=$(( SECONDS - step_start ))
    if [[ $rc -ne 0 ]]; then
        echo ""
        echo "============================================================" >&2
        echo "[RUN_ALL] FAILED STEP: '${step}' (exit code ${rc}, elapsed ${elapsed}s)" >&2
        echo "============================================================" >&2
        exit "$rc"
    fi

    echo "[RUN_ALL] COMPLETED STEP: '${step}' in ${elapsed}s"
    SUMMARY_LINES+=("$(printf "  - %-26s : PASS (%ds)" "$step" "$elapsed")")
}

for step in "${STEPS_TO_RUN[@]}"; do
    run_step "$step"
done

TOTAL_ELAPSED=$(( SECONDS - PIPELINE_START ))
echo ""
echo "============================================================"
echo "LAKEHOUSE PIPELINE SUMMARY"
echo "============================================================"
for line in "${SUMMARY_LINES[@]}"; do
    echo "$line"
done
echo "------------------------------------------------------------"
echo "  TOTAL ELAPSED TIME           : ${TOTAL_ELAPSED}s"
echo "============================================================"
