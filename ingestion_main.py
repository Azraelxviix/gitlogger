import base64
import json
import uuid
import datetime
import sys
import os

from flask import Flask, request
from google.api_core.exceptions import PreconditionFailed

# Import shared GCS resources and configuration
from gcs_utils import bucket, FRAGMENTS_PREFIX

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_log_ingestion():
    """
    FAST PATH: Handles incoming Pub/Sub messages.
    Writes each log entry as a unique, immutable fragment file to GCS.
    This endpoint is designed to be fast, stateless, and idempotent.
    """
    if bucket is None:
        print("CRITICAL: GCS bucket is not initialized. Service is misconfigured.", file=sys.stderr)
        return "Service misconfigured", 500

    # 1. Parse the incoming Pub/Sub message envelope
    envelope = request.get_json(silent=True)
    if not envelope or not isinstance(envelope, dict) or "message" not in envelope:
        print(f"Invalid Pub/Sub envelope format. Discarding message.", file=sys.stderr)
        return "Invalid request format, acknowledged.", 200 # Acknowledge to prevent redelivery

    message = envelope['message']
    pubsub_data = message.get('data', '')

    try:
        decoded_data = base64.b64decode(pubsub_data).decode('utf-8')
        log_entry = json.loads(decoded_data)
    except Exception as e:
        print(f"Error decoding/parsing Pub/Sub data, discarding message: {e}", file=sys.stderr)
        return "Malformed data, acknowledged.", 200 # Acknowledge to prevent redelivery

    # 2. Write the log entry as a unique fragment to GCS
    try:
        timestamp = log_entry.get("timestamp", datetime.datetime.now(datetime.timezone.utc).isoformat())
        message_id = message.get('message_id', str(uuid.uuid4()))

        safe_timestamp = timestamp.replace(':', '-')
        fragment_name = f"{FRAGMENTS_PREFIX}{safe_timestamp}_{message_id}.json"
        fragment_blob = bucket.blob(fragment_name)

        fragment_blob.upload_from_string(
            json.dumps(log_entry),
            content_type="application/json",
            if_generation_match=0 # Atomically create; fails if fragment already exists
        )
    except PreconditionFailed:
        # This is an expected race condition if Pub/Sub sends a duplicate message.
        # The fragment already exists, so we acknowledge the message as successfully processed.
        print(f"Duplicate message detected. Fragment {fragment_name} already exists.", file=sys.stderr)
        return "Duplicate acknowledged", 200
    except Exception as e:
        # For any other GCS error, we MUST return a 500 status. This signals to
        # Pub/Sub that the message was not processed and should be redelivered later.
        print(f"CRITICAL: Failed to write log fragment to GCS: {e}", file=sys.stderr)
        return "Internal Server Error", 500

    return "Fragment written", 200

if __name__ == "__main__":
    # This block is for local development and is not used by Gunicorn in production.
    # It requires the MASTER_LOG_BUCKET environment variable to be set.
    print("Running with Flask development server. Use Gunicorn in production.", file=sys.stderr)
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
