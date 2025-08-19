import functions_framework
from google.cloud import storage
import json

INVALID_FILES_BUCKET = "cloud-function-demo-dump-bucket"

@functions_framework.cloud_event
def check_file_size_and_move(cloud_event):
    """Triggered by a change to a GCS bucket (Gen 2 / Eventarc)."""
    data = cloud_event.data

    source_bucket_name = data['bucket']
    file_name = data['name']

    # Initialize GCS client
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    blob = source_bucket.blob(file_name)

    blob.reload()
    file_size = blob.size

    if file_size is None:
        print(f"Unable to determine the size for {file_name}. Skipping.")
        return

    max_size = 2 * 1024 * 1024  # 2MB

    if file_size > max_size:
        destination_bucket = storage_client.bucket(INVALID_FILES_BUCKET)
        new_blob_name = f"large_files/{file_name}"
        source_bucket.copy_blob(blob, destination_bucket, new_blob_name)
        blob.delete()
        print(f"Moved {file_name} to {INVALID_FILES_BUCKET}/large_files/")
    else:
        print(f"{file_name} is within size limit.")
