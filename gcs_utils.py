import os
import sys
from google.cloud import storage

# --- Configuration ---
# The GCS bucket for storing logs. This must be set via environment variable.
MASTER_LOG_BUCKET = os.environ.get("MASTER_LOG_BUCKET")

# Prefixes for organizing objects within the bucket.
FRAGMENTS_PREFIX = "fragments/"
PROCESSED_FRAGMENTS_PREFIX = "processed/"
MASTER_LOG_FILE_NAME = "master_log.json"

# --- Global GCS Client Initialization ---
# Initialize the GCS client and bucket globally to leverage connection pooling
# and avoid re-initialization on every function invocation, which is a performance best practice.
storage_client = None
bucket = None
try:
    if not MASTER_LOG_BUCKET:
        raise ValueError("CRITICAL: MASTER_LOG_BUCKET environment variable not set.")
    storage_client = storage.Client()
    bucket = storage_client.bucket(MASTER_LOG_BUCKET)
except Exception as e:
    # If initialization fails, log a critical error. Subsequent service calls
    # that depend on 'bucket' or 'storage_client' will fail, indicating a
    # fundamental misconfiguration.
    print(f"CRITICAL: Failed to initialize GCS Client or Bucket: {e}", file=sys.stderr)
