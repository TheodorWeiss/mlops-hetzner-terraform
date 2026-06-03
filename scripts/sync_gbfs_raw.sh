#!/usr/bin/env bash
set -euo pipefail

echo "=== BikeML GBFS raw sync started at $(date -Iseconds) ==="

: "${INGESTION_SSH_HOST:?INGESTION_SSH_HOST is required}"
: "${INGESTION_SSH_USER:?INGESTION_SSH_USER is required}"
: "${INGESTION_RAW_PATH:?INGESTION_RAW_PATH is required}"
: "${INGESTION_SSH_KEY_PATH:?INGESTION_SSH_KEY_PATH is required}"

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${MINIO_BUCKET_RAW:?MINIO_BUCKET_RAW is required}"

LOCAL_RAW_ROOT="/mnt/mlops-data/incoming-raw/gbfs"
MINIO_ALIAS="local"
MINIO_ENDPOINT="http://minio:9000"

SSH_OPTS="-i ${INGESTION_SSH_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/tmp/bikeml_known_hosts"

mkdir -p "${LOCAL_RAW_ROOT}/latest"
mkdir -p "${LOCAL_RAW_ROOT}/station_status"
mkdir -p "${LOCAL_RAW_ROOT}/station_information"

echo "--- Sync latest files: overwrite allowed ---"
rsync -rtvz --no-perms --no-owner --no-group \
  -e "ssh ${SSH_OPTS}" \
  "${INGESTION_SSH_USER}@${INGESTION_SSH_HOST}:${INGESTION_RAW_PATH}/latest/" \
  "${LOCAL_RAW_ROOT}/latest/"

echo "--- Sync station_status archive: immutable files, ignore existing ---"
rsync -rtvz --no-perms --no-owner --no-group --ignore-existing \
  -e "ssh ${SSH_OPTS}" \
  "${INGESTION_SSH_USER}@${INGESTION_SSH_HOST}:${INGESTION_RAW_PATH}/station_status/" \
  "${LOCAL_RAW_ROOT}/station_status/"

echo "--- Sync station_information archive: immutable files, ignore existing ---"
rsync -rtvz --no-perms --no-owner --no-group --ignore-existing \
  -e "ssh ${SSH_OPTS}" \
  "${INGESTION_SSH_USER}@${INGESTION_SSH_HOST}:${INGESTION_RAW_PATH}/station_information/" \
  "${LOCAL_RAW_ROOT}/station_information/"

echo "--- Configure MinIO alias ---"
mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null

echo "--- Mirror station_status to MinIO raw bucket ---"
mc mirror \
  "${LOCAL_RAW_ROOT}/station_status" \
  "${MINIO_ALIAS}/${MINIO_BUCKET_RAW}/gbfs/station_status"

echo "--- Mirror station_information to MinIO raw bucket ---"
mc mirror \
  "${LOCAL_RAW_ROOT}/station_information" \
  "${MINIO_ALIAS}/${MINIO_BUCKET_RAW}/gbfs/station_information"

echo "--- Mirror latest to MinIO raw bucket ---"
mc mirror --overwrite \
  "${LOCAL_RAW_ROOT}/latest" \
  "${MINIO_ALIAS}/${MINIO_BUCKET_RAW}/gbfs/latest"


echo "--- Local staging counts ---"
echo -n "station_status local files: "
find "${LOCAL_RAW_ROOT}/station_status" -type f -name "*.json.gz" | wc -l

echo -n "station_information local files: "
find "${LOCAL_RAW_ROOT}/station_information" -type f -name "*.json.gz" | wc -l

echo "--- MinIO counts ---"
echo -n "station_status MinIO files: "
mc find "${MINIO_ALIAS}/${MINIO_BUCKET_RAW}/gbfs/station_status" --name "*.json.gz" | wc -l

echo -n "station_information MinIO files: "
mc find "${MINIO_ALIAS}/${MINIO_BUCKET_RAW}/gbfs/station_information" --name "*.json.gz" | wc -l

echo "--- Latest station_status key in MinIO ---"
mc find "${MINIO_ALIAS}/${MINIO_BUCKET_RAW}/gbfs/station_status" --name "*.json.gz" | sort | tail -n 1

echo "=== BikeML GBFS raw sync finished at $(date -Iseconds) ==="
