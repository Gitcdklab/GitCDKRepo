import boto3
import io
import os
import zipfile
import csv
from datetime import datetime
import sys
from awsglue.utils import getResolvedOptions

# Get required parameters
params = getResolvedOptions(sys.argv, ["bucket_name", "source_prefix", "dest_prefix"])

# Optional parameter
zip_name = ""
if "zip_name" in sys.argv:
    try:
        zip_name = getResolvedOptions(sys.argv, ["zip_name"]).get("zip_name", "")
    except:
        zip_name = ""

# Assign parameters with normalization
bucket_name = params["bucket_name"]
source_prefix = params["source_prefix"]
dest_prefix = params["dest_prefix"].rstrip("/") + "/"

s3 = boto3.client("s3")

def list_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    objects = []
    for page in pages:
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):  # skip folders
                objects.append(obj)
    return objects

def create_zip(bucket, objects):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        manifest = []
        for obj in objects:
            key = obj["Key"]
            size = obj["Size"]
            new_name = f"{os.path.splitext(os.path.basename(key))[0]}__{size}B{os.path.splitext(key)[1]}"
            body = s3.get_object(Bucket=bucket, Key=key)["Body"]
            with zf.open(new_name, "w") as f:
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    f.write(chunk)
            manifest.append([key, new_name, size])
        manifest_io = io.StringIO()
        writer = csv.writer(manifest_io)
        writer.writerow(["original_key", "new_name", "size_bytes"])
        writer.writerows(manifest)
        zf.writestr("manifest.csv", manifest_io.getvalue())
    zip_buffer.seek(0)
    return zip_buffer

def upload_zip(bucket, dest_prefix, zip_buffer, zip_name):
    if not zip_name:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        zip_name = f"archive_{ts}.zip"
    dest_key = f"{dest_prefix}{zip_name}"
    s3.upload_fileobj(zip_buffer, bucket, dest_key)
    print(f"Uploaded ZIP to s3://{bucket}/{dest_key}")

def delete_source_files(bucket, objects):
    for obj in objects:
        key = obj["Key"]
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted source file: s3://{bucket}/{key}")

def main():
    print(f"Listing files from s3://{bucket_name}/{source_prefix}")
    objects = list_objects(bucket_name, source_prefix)
    if not objects:
        print("No files found.")
        return
    print(f"Found {len(objects)} files. Creating ZIP...")
    zip_buffer = create_zip(bucket_name, objects)
    upload_zip(bucket_name, dest_prefix, zip_buffer, zip_name)
    print("Deleting source files...")
    delete_source_files(bucket_name, objects)

if __name__ == "__main__":
    main()
