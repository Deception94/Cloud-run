import os
import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1
import json
import base64

# Get the Pub/Sub topic ID from environment variables
# This makes our function more flexible and reusable.
PROJECT_ID = os.environ.get('GCP_PROJECT')
# Replace with the actual name of your Pub/Sub topic created earlier
# e.g., 'file-upload-info-topic'
PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_TOPIC_ID')

if not PROJECT_ID or not PUBSUB_TOPIC_ID:
    print("Error: GCP_PROJECT or PUBSUB_TOPIC_ID environment variables are not set.")
    # Exit or handle this error appropriately in a production environment
    # For this assignment, ensure they are set during deployment.

# Initialize the Pub/Sub publisher client
# This client is used to send messages to our Pub/Sub topic.
publisher = pubsub_v1.PublisherClient()
# Construct the full topic path
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)

@functions_framework.cloud_event
def process_gcs_event(cloud_event: CloudEvent):
    """
    This function is triggered by a Cloud Storage event.
    It extracts file metadata and publishes it to a Pub/Sub topic.

    Args:
        cloud_event: The CloudEvent object representing the GCS event.
                     This object contains information about the triggered event.
    """
    print(f"Received a Cloud Storage event: {cloud_event.attributes}")

    # Extract data from the CloudEvent
    # The CloudEvent data payload contains information about the GCS object.
    data = cloud_event.get_data()

    if not data:
        print("No data found in the CloudEvent. Exiting.")
        return

    # Decode the base64 encoded data payload if it's from a GCS event.
    # Cloud Storage events usually have a base64 encoded data payload.
    # The functions_framework library handles some of this, but it's good to be aware.
    try:
        if isinstance(data, dict):
            # If data is already a dict, it might be directly accessible
            payload = data
        else:
            # If data is bytes or string, try to decode it
            payload_str = data.decode('utf-8') if isinstance(data, bytes) else data
            payload = json.loads(payload_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON payload: {e}")
        print(f"Raw data: {data}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while processing data: {e}")
        return

    # Extract relevant file information
    file_name = payload.get('name')
    bucket_name = payload.get('bucket')
    file_size = payload.get('size') # Size is usually a string from GCS events
    content_type = payload.get('contentType') # This gives the format (e.g., image/jpeg, application/pdf)

    # Log the extracted information to Cloud Run logs
    print(f"Processing file: {file_name}")
    print(f"  Bucket: {bucket_name}")
    print(f"  Size: {file_size} bytes")
    print(f"  Format (Content-Type): {content_type}")

    # Prepare the message payload for Pub/Sub
    # We're creating a JSON string to send as the message data.
    message_payload = {
        'fileName': file_name,
        'fileSize': file_size,
        'fileFormat': content_type
    }
    # Convert dictionary to a JSON string, then encode it to bytes
    message_data = json.dumps(message_payload).encode('utf-8')

    # Publish the message to Pub/Sub
    try:
        future = publisher.publish(topic_path, message_data)
        message_id = future.result() # Blocks until the message is published
        print(f"Published message with ID: {message_id} to topic: {PUBSUB_TOPIC_ID}")
    except Exception as e:
        print(f"Failed to publish message to Pub/Sub: {e}")

