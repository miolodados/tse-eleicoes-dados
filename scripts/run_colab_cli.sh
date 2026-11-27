#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Unified Reproducible Notebook Runner via Google Colab CLI
# ==============================================================================
# Uses ONE persistent Colab session (default: `tse-colab`), mounts Google Drive
# ONCE, injects KAGGLE_API_TOKEN from environment / local secure cache (never
# hardcoded in source), cleans /tmp staging & orphan *.parquet.tmp, and executes
# any phased notebook with automatic kernel-restart resume (preserving Drive).
#
# Usage:
#   ./scripts/run_colab_cli.sh [NOTEBOOK_PATH_OR_ID] [SESSION_NAME] [OPTIONAL_UFS_CSV] [OPTIONAL_TURNS_CSV]
#
# Examples:
#   ./scripts/run_colab_cli.sh 02 tse-nb02
#   ./scripts/run_colab_cli.sh 03 tse-nb03
#   ./scripts/run_colab_cli.sh 03 tse-nb03 "AC,AL,AM" "1,2"
#   ./scripts/run_colab_cli.sh notebooks/05_silver_fact_urna_event/05_silver_fact_urna_event.ipynb tse-silver
# ==============================================================================

NOTEBOOK_INPUT="${1:-notebooks/03_ingest_urna_logs_batch/03_ingest_urna_logs_batch.ipynb}"
SESSION_NAME="${2:-tse-colab}"
CUSTOM_UFS="${3:-${TSE_TARGET_UFS:-}}"
CUSTOM_TURNS="${4:-${TSE_TARGET_TURNS:-}}"

if ! command -v colab >/dev/null 2>&1; then
    echo "[ERROR] 'colab' CLI not found on your machine."
    echo "Install it via: uv tool install google-colab-cli"
    exit 1
fi

# 1. Resolve notebook path from explicit .ipynb path or shorthand ID (01..09)
resolve_notebook_path() {
    local input="$1"
    if [[ -f "${input}" && "${input}" == *.ipynb ]]; then
        printf "%s" "${input}"
        return 0
    fi
    case "${input}" in
        01|1) printf "%s" "notebooks/01_urna_logs_poc/01_urna_logs_poc.ipynb" ;;
        02|2) printf "%s" "notebooks/02_ingest_electoral_tables/02_ingest_electoral_tables.ipynb" ;;
        03|3) printf "%s" "notebooks/03_ingest_urna_logs_batch/03_ingest_urna_logs_batch.ipynb" ;;
        04|4) printf "%s" "notebooks/04_dataset_integrity_and_release/04_dataset_integrity_and_release.ipynb" ;;
        05|5) printf "%s" "notebooks/05_silver_fact_urna_event/05_silver_fact_urna_event.ipynb" ;;
        06|6) printf "%s" "notebooks/06_silver_fact_voting_session/06_silver_fact_voting_session.ipynb" ;;
        07|7) printf "%s" "notebooks/07_silver_dimensions/07_silver_dimensions.ipynb" ;;
        08|8) printf "%s" "notebooks/08_gold_analytics/08_gold_analytics.ipynb" ;;
        09|9) printf "%s" "notebooks/09_release_pipeline/09_release_pipeline.ipynb" ;;
        *)
            local matched
            matched="$(find notebooks -maxdepth 2 -type f -name "*${input}*.ipynb" ! -name "*_output.ipynb" | sort | head -n 1 || true)"
            if [[ -n "${matched}" && -f "${matched}" ]]; then
                printf "%s" "${matched}"
            else
                return 1
            fi
            ;;
    esac
}

NOTEBOOK_PATH="$(resolve_notebook_path "${NOTEBOOK_INPUT}" || true)"
if [[ -z "${NOTEBOOK_PATH}" || ! -f "${NOTEBOOK_PATH}" || "${NOTEBOOK_PATH}" != *.ipynb ]]; then
    echo "[ERROR] Could not resolve notebook '${NOTEBOOK_INPUT}'."
    echo "Usage: ./scripts/run_colab_cli.sh [NOTEBOOK_PATH_OR_ID] [SESSION_NAME] [OPTIONAL_UFS_CSV] [OPTIONAL_TURNS_CSV]"
    exit 1
fi

# 2. Load KAGGLE_API_TOKEN from env, cached secret file (~/.config/kaggle/colab_token.env), or prompt once
LOCAL_KAGGLE_TOKEN="${KAGGLE_API_TOKEN:-}"
if [[ -z "${LOCAL_KAGGLE_TOKEN}" && -f "${HOME}/.config/kaggle/colab_token.env" ]]; then
    LOCAL_KAGGLE_TOKEN="$(cat "${HOME}/.config/kaggle/colab_token.env")"
fi

if [[ -z "${LOCAL_KAGGLE_TOKEN}" && -t 0 ]]; then
    read -rsp "Paste your KAGGLE_API_TOKEN (hidden, cached in ~/.config/kaggle/colab_token.env): " INPUT_TOKEN
    echo ""
    if [[ -n "${INPUT_TOKEN}" ]]; then
        LOCAL_KAGGLE_TOKEN="${INPUT_TOKEN}"
        mkdir -p "${HOME}/.config/kaggle"
        printf "%s" "${LOCAL_KAGGLE_TOKEN}" > "${HOME}/.config/kaggle/colab_token.env"
        chmod 600 "${HOME}/.config/kaggle/colab_token.env"
    fi
fi

if [[ -z "${LOCAL_KAGGLE_TOKEN}" ]]; then
    echo "[ERROR] KAGGLE_API_TOKEN is required. Export KAGGLE_API_TOKEN or cache it in ~/.config/kaggle/colab_token.env"
    exit 1
fi

TOKEN_B64="$(printf "%s" "${LOCAL_KAGGLE_TOKEN}" | base64 | tr -d '\n')"
TARGET_DATASET_HANDLE="${KAGGLE_DATASET_HANDLE:-daniellmattos/tse-eleicoes-dados-v2}"
TARGET_DATASET_PATH="${DATASET_PATH:-/content/drive/MyDrive/tse-eleicoes-dados/data}"
TARGET_STAGING_PATH="${STAGING_PATH:-/tmp/tse-eleicoes-staging}"
EXEC_TIMEOUT="${TSE_COLAB_TIMEOUT:-86400}"

echo "=============================================================================="
echo "[1/4] Checking or Creating Persistent Session '${SESSION_NAME}'..."
echo "      Notebook: ${NOTEBOOK_PATH}"
echo "=============================================================================="
STATUS_OUT="$(colab status -s "${SESSION_NAME}" 2>&1 || true)"
echo "${STATUS_OUT}"

if [[ "${STATUS_OUT}" == *"not found"* ]] || [[ "${STATUS_OUT}" == *"No active sessions"* ]]; then
    echo "[INFO] Allocating persistent session '${SESSION_NAME}' via 'colab new -s ${SESSION_NAME}'..."
    colab new -s "${SESSION_NAME}"
fi

echo "=============================================================================="
echo "[2/4] Mounting Google Drive ONCE & Injecting Environment on '${SESSION_NAME}'..."
echo "=============================================================================="

check_drive_mounted() {
    colab exec -s "${SESSION_NAME}" --timeout 120 <<'EOF'
from pathlib import Path
print(f"DRIVE_OK={Path('/content/drive/MyDrive').exists()}")
EOF
}

DRIVE_STATUS="$(check_drive_mounted)"
echo "${DRIVE_STATUS}"

if [[ "${DRIVE_STATUS}" == *"DRIVE_OK=False"* ]]; then
    echo "[INFO] Mounting Google Drive (/content/drive) on '${SESSION_NAME}'..."
    colab drivemount -s "${SESSION_NAME}" /content/drive
fi

# Inject KAGGLE_API_TOKEN, unified paths, parallel tuning, and clean staging/tmp files
prepare_remote_kernel_env() {
    colab exec -s "${SESSION_NAME}" --timeout 120 <<EOF
import base64
import os
import shutil
from pathlib import Path

os.environ["KAGGLE_API_TOKEN"] = base64.b64decode("${TOKEN_B64}").decode("utf-8").strip()
os.environ["KAGGLE_DATASET_HANDLE"] = "${TARGET_DATASET_HANDLE}"
os.environ["DATASET_PATH"] = "${TARGET_DATASET_PATH}"
os.environ["STAGING_PATH"] = "${TARGET_STAGING_PATH}"
os.environ["TSE_PREFETCH_DOWNLOADERS"] = "${TSE_PREFETCH_DOWNLOADERS:-3}"
os.environ["TSE_MAX_PARQUET_PART_BYTES"] = "${TSE_MAX_PARQUET_PART_BYTES:-134217728}"
os.environ["TSE_MANIFEST_NAME"] = "${TSE_MANIFEST_NAME:-manifest.json}"
os.environ["TSE_PROC_WORKERS"] = "${TSE_PROC_WORKERS:-8}"
os.environ["TSE_CHUNK_JEZ_SIZE"] = "${TSE_CHUNK_JEZ_SIZE:-500}"

for env_key, env_val in (
    ("TSE_TARGET_TURNS", "${CUSTOM_TURNS}"),
    ("TSE_TARGET_UFS", "${CUSTOM_UFS}"),
    ("TSE_SKIP_KAGGLE_RELEASE", "${TSE_SKIP_KAGGLE_RELEASE:-}"),
    ("TSE_MAX_JEZ_FILES", "${TSE_MAX_JEZ_FILES:-}"),
    ("TSE_AUDIT_UFS", "${TSE_AUDIT_UFS:-}"),
    ("TSE_PUBLISH_POC", "${TSE_PUBLISH_POC:-}"),
):
    if env_val != "":
        os.environ[env_key] = env_val
    else:
        os.environ.pop(env_key, None)

token_cache = Path("/root/.config/kaggle/colab_token.env")
token_cache.parent.mkdir(parents=True, exist_ok=True)
token_cache.write_text(os.environ["KAGGLE_API_TOKEN"], encoding="utf-8")
token_cache.chmod(0o600)

assert Path("/content/drive/MyDrive").exists(), "Google Drive mount not found!"

staging = Path(os.environ["STAGING_PATH"])
if staging.exists():
    shutil.rmtree(staging, ignore_errors=True)
staging.mkdir(parents=True, exist_ok=True)

data_dir = Path(os.environ["DATASET_PATH"])
removed_tmps = 0
if data_dir.exists():
    for tmp_pq in data_dir.rglob("*.parquet.tmp"):
        tmp_pq.unlink(missing_ok=True)
        removed_tmps += 1

print(
    f"[REMOTE-ENV] Ready on session '${SESSION_NAME}' | "
    f"CPUs={os.cpu_count()} | handle={os.environ['KAGGLE_DATASET_HANDLE']} | "
    f"UFs={os.environ.get('TSE_TARGET_UFS', 'ALL_28_UFS')} | "
    f"staging_clean=True | removed_tmps={removed_tmps}"
)
EOF
}

prepare_remote_kernel_env

echo "=============================================================================="
echo "[3/4] Installing/Verifying Python dependencies on '${SESSION_NAME}'..."
echo "=============================================================================="
colab install -s "${SESSION_NAME}" "kagglehub>=0.4.0" polars pyarrow duckdb httpx py7zr pyyaml || true

echo "=============================================================================="
echo "[4/4] Executing ${NOTEBOOK_PATH} on Session '${SESSION_NAME}' (--timeout ${EXEC_TIMEOUT})"
echo "=============================================================================="
LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "${LOG_DIR}"
NB_BASENAME="$(basename "${NOTEBOOK_PATH%.ipynb}")"
LOG_FILE="${LOG_DIR}/colab_${NB_BASENAME}_$(date +%Y%m%d_%H%M%S).log"

verify_attempt_success() {
    local attempt_log="$1"
    case "${NB_BASENAME}" in
        02_ingest_electoral_tables|03_ingest_urna_logs_batch)
            grep -q "Auditoria concluída com 100% de sucesso" "${attempt_log}" \
                && grep -q "\[SUCESSO\] Nova versão" "${attempt_log}"
            ;;
        01_urna_logs_poc)
            grep -q "poc_validation_summary.json" "${attempt_log}"
            ;;
        *)
            grep -q "\[SUCESSO\] Nova versão" "${attempt_log}"
            ;;
    esac
}

MAX_ATTEMPTS="${MAX_ATTEMPTS:-15}"
for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
    echo ">>> [Attempt ${attempt}/${MAX_ATTEMPTS}] $(date -Iseconds)" | tee -a "${LOG_FILE}"

    # Refresh local proxy token; use restart-kernel (never colab stop) to preserve Drive mount
    colab sessions >/dev/null 2>&1 || true
    if (( attempt > 1 )); then
        echo "[INFO] Restarting remote kernel on '${SESSION_NAME}' to clear interrupted cell (preserving Drive mount)..." | tee -a "${LOG_FILE}"
        colab restart-kernel -s "${SESSION_NAME}" || true
        sleep 3
    fi
    prepare_remote_kernel_env 2>&1 | tee -a "${LOG_FILE}"

    ATTEMPT_LOG="$(mktemp)"
    if colab exec -s "${SESSION_NAME}" --timeout "${EXEC_TIMEOUT}" -f "${NOTEBOOK_PATH}" 2>&1 | tee "${ATTEMPT_LOG}" | tee -a "${LOG_FILE}" \
        && verify_attempt_success "${ATTEMPT_LOG}"; then
        rm -f "${ATTEMPT_LOG}"
        echo "=============================================================================="
        echo "[SUCCESS] ${NB_BASENAME} completed on session '${SESSION_NAME}'!"
        echo "          Output notebook: ${NOTEBOOK_PATH%.ipynb}_output.ipynb"
        echo "          Full log       : ${LOG_FILE}"
        echo "=============================================================================="
        exit 0
    fi
    rm -f "${ATTEMPT_LOG}"

    echo "[WARN] Attempt ${attempt} interrupted. Resuming on SAME session '${SESSION_NAME}' in 5s..." | tee -a "${LOG_FILE}"
    sleep 5
done

echo "[ERROR] Execution failed after ${MAX_ATTEMPTS} attempts. Check ${LOG_FILE}."
exit 1
