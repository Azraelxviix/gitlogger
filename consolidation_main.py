import json
import datetime
import sys
from google.api_core.exceptions import PreconditionFailed, NotFound

# Import shared GCS resources and configuration
from gcs_utils import (
    bucket,
    storage_client,
    FRAGMENTS_PREFIX,
    PROCESSED_FRAGMENTS_PREFIX,
    MASTER_LOG_FILE_NAME,
)

# Configuration for the consolidation logic
MAX_LOG_SIZE_KB = 200

def handle_consolidation(event, context):
    """
    Triggered by Eventarc (Cloud Scheduler). Consolidates fragments into master_log.json.
    NOTE: Concurrency is controlled by the Cloud Function's max_instances=1 setting,
    which makes application-level locking unnecessary.
    """
    if bucket is None or storage_client is None:
        print("CRITICAL: GCS bucket/client not initialized. Aborting.", file=sys.stderr)
        # Return 2xx to prevent the trigger from retrying a fundamentally broken configuration.
        # The error is logged for monitoring and alerting.
        return "Service misconfigured", 204

    # 1. List all available fragments.
    fragment_blobs = list(bucket.list_blobs(prefix=FRAGMENTS_PREFIX))
    if not fragment_blobs:
        print("No fragments to consolidate.")
        return "No fragments to consolidate.", 200

    # 2. Read the current master log, capturing its generation for a conditional write.
    master_blob = bucket.blob(MASTER_LOG_FILE_NAME)
    all_logs = []
    generation = 0
    try:
        content = master_blob.download_as_text()
        generation = master_blob.generation
        all_logs = json.loads(content)
        if not isinstance(all_logs, list):
            print(f"Master log is not a list. It will be treated as corrupt.", file=sys.stderr)
            all_logs = []
    except NotFound:
        print("Master log not found. A new one will be created.")
    except json.JSONDecodeError:
        print("Could not parse master log. It will be overwritten.", file=sys.stderr)
    except Exception as e:
        print(f"CRITICAL: Unrecoverable error reading master log: {e}", file=sys.stderr)
        raise  # Re-raise to have the function fail and trigger a potential retry.

    # 3. Process all fragments into a sorted list of new entries.
    new_entries = []
    valid_fragment_blobs = []
    for fragment in fragment_blobs:
        if fragment.name.endswith('/'): continue # Skip "directories"
        try:
            fragment_data = json.loads(fragment.download_as_text())
            new_entries.append(fragment_data)
            valid_fragment_blobs.append(fragment)
        except Exception as e:
            print(f"Warning: Error processing fragment {fragment.name}: {e}. Skipping.", file=sys.stderr)

    if not new_entries:
        print("No valid entries found in fragments.")
        return "No valid entries found in fragments.", 200

    new_entries.sort(key=lambda x: x.get("timestamp", "1970-01-01T00:00:00Z"))

    # 4. Handle log rotation logic.
    logs_to_archive = None
    current_content_str = json.dumps(all_logs)
    if (len(current_content_str.encode('utf-8')) / 1024) > MAX_LOG_SIZE_KB:
        logs_to_archive = json.loads(current_content_str)
        logs_for_main_file = new_entries
        generation = 0  # Writing a new file, so no existing generation to match.
    else:
        logs_for_main_file = all_logs + new_entries

    # 5. Atomically write the updated master log.
    try:
        master_blob.upload_from_string(
            json.dumps(logs_for_main_file, indent=2),
            content_type="application/json",
            if_generation_match=generation,
        )
    except PreconditionFailed:
        print("CRITICAL: Master log modified unexpectedly. This should not happen with max_instances=1. Aborting.", file=sys.stderr)
        raise # Fail the function to signal a critical, unexpected error.
    except Exception as e:
        print(f"CRITICAL: Failed to write master log: {e}", file=sys.stderr)
        raise # Fail the function

    # 6. Archive old log if rotation occurred.
    if logs_to_archive:
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
        archive_blob = bucket.blob(f"archive/master_log_{timestamp}.json")
        try:
            archive_blob.upload_from_string(json.dumps(logs_to_archive, indent=2))
        except Exception as e:
            print(f"Warning: Failed to upload archive file: {e}", file=sys.stderr)

    # 7. Robust Cleanup: Move processed fragments to an archive folder.
    processed_count = 0
    try:
        # This is a non-transactional move, but it's more robust than simple deletion.
        # First, copy all blobs to their new destination.
        for blob in valid_fragment_blobs:
            new_name = f"{PROCESSED_FRAGMENTS_PREFIX}{blob.name[len(FRAGMENTS_PREFIX):]}"
            bucket.copy_blob(blob, bucket, new_name)

        # After all copies succeed, delete the originals in a single batch operation.
        with storage_client.batch():
            for blob in valid_fragment_blobs:
                blob.delete()

        processed_count = len(valid_fragment_blobs)
        print(f"Successfully processed and moved {processed_count} fragments.")
    except Exception as e:
        # This warning is critical. It means fragments were consolidated but not moved,
        # so they will be re-processed on the next run, causing duplicates.
        print(f"CRITICAL WARNING: Failed during fragment cleanup. Duplicates will occur on next run. Error: {e}", file=sys.stderr)

    return f"Consolidation complete. Processed {processed_count} fragments.", 200
