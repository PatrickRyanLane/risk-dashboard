"""
Google Cloud Storage utilities for reading/writing CSV files and text content.
This replaces local file operations with cloud storage operations.
"""

from google.cloud import storage
import pandas as pd
import io
import os


class CloudStorageManager:
    """
    Manages file operations with Google Cloud Storage
    
    Usage:
        storage = CloudStorageManager(bucket_name='my-bucket')
        
        # Write CSV
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        storage.write_csv(df, 'data/myfile.csv')
        
        # Read CSV
        df = storage.read_csv('data/myfile.csv')
        
        # Write/read text (for JSON checkpoints, etc.)
        storage.write_text('{"key": "value"}', 'data/checkpoint.json')
        content = storage.read_text('data/checkpoint.json')
        
        # List files
        files = storage.list_files('data/')
    """
    
    def __init__(self, bucket_name="risk-dashboard"):
        """
        Initialize storage manager
        
        Args:
            bucket_name: Name of your GCS bucket. If None, uses PROJECT_ID-sentiment-data
        """
        self.client = storage.Client()
        
        # Auto-generate bucket name if not provided
        if bucket_name is None:
            project_id = os.environ.get('GCP_PROJECT', 'your-project-id')
            bucket_name = f"{project_id}-sentiment-data"
        
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)
        
        # Create bucket if it doesn't exist
        if not self.bucket.exists():
            print(f"Creating bucket: {bucket_name}")
            self.bucket = self.client.create_bucket(bucket_name, location='us-central1')
            print(f"✅ Bucket created: {bucket_name}")
    
    # -------------------------------------------------------------------------
    # CSV Operations
    # -------------------------------------------------------------------------
    
    def write_csv(self, df, filepath, index=False):
        """
        Write pandas DataFrame to GCS as CSV
        
        Args:
            df: pandas DataFrame
            filepath: Path in bucket (e.g., 'data/daily_counts/2024-01-01.csv')
            index: Whether to include DataFrame index in CSV
        
        Returns:
            GCS URI of the uploaded file
        """
        csv_string = df.to_csv(index=index)
        blob = self.bucket.blob(filepath)
        blob.upload_from_string(csv_string, content_type='text/csv')
        print(f"✅ Uploaded: gs://{self.bucket_name}/{filepath}")
        return f"gs://{self.bucket_name}/{filepath}"
    
    def read_csv(self, filepath, **kwargs):
        """
        Read CSV from GCS into pandas DataFrame
        
        Args:
            filepath: Path in bucket (e.g., 'data/daily_counts/2024-01-01.csv')
            **kwargs: Additional arguments for pd.read_csv()
        
        Returns:
            pandas DataFrame
        """
        blob = self.bucket.blob(filepath)
        
        if not blob.exists():
            raise FileNotFoundError(f"File not found: gs://{self.bucket_name}/{filepath}")
        
        csv_string = blob.download_as_string()
        df = pd.read_csv(io.StringIO(csv_string.decode('utf-8')), **kwargs)
        print(f"✅ Read: gs://{self.bucket_name}/{filepath} ({len(df)} rows)")
        return df
    
    # -------------------------------------------------------------------------
    # Text Operations (for JSON checkpoints, config files, etc.)
    # -------------------------------------------------------------------------
    
    def write_text(self, content, filepath, content_type='application/json'):
        """
        Write text content to GCS (useful for JSON checkpoints, config files)
        
        Args:
            content: String content to write
            filepath: Path in bucket (e.g., 'data/checkpoints/checkpoint.json')
            content_type: MIME type (default: application/json)
        
        Returns:
            GCS URI of the uploaded file
        """
        blob = self.bucket.blob(filepath)
        blob.upload_from_string(content, content_type=content_type)
        print(f"✅ Uploaded: gs://{self.bucket_name}/{filepath}")
        return f"gs://{self.bucket_name}/{filepath}"
    
    def read_text(self, filepath):
        """
        Read text content from GCS
        
        Args:
            filepath: Path in bucket (e.g., 'data/checkpoints/checkpoint.json')
        
        Returns:
            String content of the file
        """
        blob = self.bucket.blob(filepath)
        
        if not blob.exists():
            raise FileNotFoundError(f"File not found: gs://{self.bucket_name}/{filepath}")
        
        return blob.download_as_text()
    
    # -------------------------------------------------------------------------
    # File Management
    # -------------------------------------------------------------------------
    
    def file_exists(self, filepath):
        """Check if a file exists in GCS"""
        blob = self.bucket.blob(filepath)
        return blob.exists()
    
    def list_files(self, prefix='', delimiter=None):
        """
        List files in bucket with optional prefix
        
        Args:
            prefix: Only list files starting with this prefix (e.g., 'data/daily_counts/')
            delimiter: If set, emulates folder structure (use '/')
        
        Returns:
            List of blob names (file paths)
        """
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix, delimiter=delimiter)
        files = [blob.name for blob in blobs]
        print(f"Found {len(files)} files with prefix '{prefix}'")
        return files
    
    def delete_file(self, filepath):
        """Delete a file from GCS"""
        blob = self.bucket.blob(filepath)
        if blob.exists():
            blob.delete()
            print(f"🗑️  Deleted: gs://{self.bucket_name}/{filepath}")
        else:
            print(f"⚠️  File not found (skipping delete): gs://{self.bucket_name}/{filepath}")
    
    def copy_file(self, source_filepath, dest_filepath):
        """Copy a file within the same bucket"""
        source_blob = self.bucket.blob(source_filepath)
        self.bucket.copy_blob(source_blob, self.bucket, dest_filepath)
        print(f"📋 Copied: {source_filepath} → {dest_filepath}")
    
    # -------------------------------------------------------------------------
    # URL Helpers
    # -------------------------------------------------------------------------
    
    def get_public_url(self, filepath):
        """
        Get public URL for a file (bucket must be public)
        
        Args:
            filepath: Path in bucket
        
        Returns:
            Public HTTPS URL
        """
        return f"https://storage.googleapis.com/{self.bucket_name}/{filepath}"
    
    def make_public(self, filepath):
        """Make a specific file publicly accessible"""
        blob = self.bucket.blob(filepath)
        blob.make_public()
        print(f"🌐 Made public: {self.get_public_url(filepath)}")
        return self.get_public_url(filepath)


if __name__ == '__main__':
    # Quick test
    print("CloudStorageManager ready. Available methods:")
    print("  - write_csv(df, filepath)")
    print("  - read_csv(filepath)")
    print("  - write_text(content, filepath)")
    print("  - read_text(filepath)")
    print("  - file_exists(filepath)")
    print("  - delete_file(filepath)")
    print("  - list_files(prefix)")
