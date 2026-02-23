import io
import os

from google.cloud import storage


def parse_gcs_path(path):
    if not path.startswith("gs://"):
        return None, None
    raw = path[5:]
    if "/" not in raw:
        return raw, ""
    bucket, blob = raw.split("/", 1)
    return bucket, blob


def open_gcs_text(path):
    bucket_name, blob_name = parse_gcs_path(path)
    if not bucket_name:
        raise ValueError("Not a gs:// path")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    return io.StringIO(data.decode("utf-8"))


def exists_gcs(path):
    bucket_name, blob_name = parse_gcs_path(path)
    if not bucket_name:
        return False
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_name).exists()
