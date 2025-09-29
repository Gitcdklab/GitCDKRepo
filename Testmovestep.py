import sys
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple, List

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext

# ------------------ Param helpers handling exception for parameters------------------
def get_arg(name: str, default: Optional[str] = None) -> Optional[str]:
    try:
        return getResolvedOptions(sys.argv, [name])[name]
    except Exception:
        return default

# ------------------ Boto3 client handling exception for boto3 library-------------------
BOTO_CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60)
s3 = boto3.client("s3", config=BOTO_CFG)

class S3MoveError(Exception):
    pass

# ------------------ Time helpers handling exception for time -------------------
def parse_tz_offset(offset: str) -> timezone:
    if not offset or offset.upper() in ("Z", "UTC"):
        return timezone.utc
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", offset)
    if not m:
        return timezone.utc
    sign = 1 if m.group(1) == "+" else -1
    return timezone(sign * timedelta(hours=int(m.group(2)), minutes=int(m.group(3))))

def current_ts(tz_offset: str, fmt: str) -> str:
    return datetime.now(parse_tz_offset(tz_offset)).strftime(fmt)

# ------------------ S3 helpers handling exception for s3 bucket---------------------
def head_object_safe(bucket: str, key: str) -> Optional[Dict]:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise

def list_objects(bucket: str, prefix: str) -> List[Dict]:
    """List up to 1000 objects under prefix."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return resp.get("Contents", [])

def wait_until_exists(bucket: str, key: str, timeout_s: int = 120, poll_s: float = 1.5):
    start = time.time()
    while time.time() - start < timeout_s:
        if head_object_safe(bucket, key):
            return
        time.sleep(poll_s)
    raise S3MoveError(f"Timed out waiting for s3://{bucket}/{key}")

def find_single_data_key_under_folder(bucket: str, folder_prefix: str) -> str:
    """
    Find exactly one data object under a Spark/Glue output folder.
    - Ignores "directories" (keys ending with '/')
    - Ignores housekeeping like _SUCCESS
    - Prefers part-* if present
    Raises if 0 or >1 candidates.
    """
    prefix = folder_prefix.rstrip("/") + "/"
    objs = list_objects(bucket, prefix)
    if not objs:
        raise FileNotFoundError(f"No objects found under s3://{bucket}/{prefix}")

    data = []
    part_candidates = []
    for o in objs:
        key = o["Key"]
        base = os.path.basename(key)
        if key.endswith("/") or base.startswith("_"):
            continue
        data.append(key)
        if base.startswith("part-"):
            part_candidates.append(key)

    candidates = part_candidates if part_candidates else data

    if len(candidates) == 0:
        raise FileNotFoundError(f"No data objects under s3://{bucket}/{prefix} (only markers?)")
    if len(candidates) > 1:
        raise RuntimeError(f"Expected exactly 1 data object under {bucket}/{prefix}, found {len(candidates)}: {candidates}")

    return candidates[0]

def source_base_from_key(key: str) -> Tuple[str, str]:
    """Return (base_without_ext, ext_with_dot_or_empty) from a key."""
    base = os.path.basename(key)
    name, ext = os.path.splitext(base)
    return name, ext  # ext includes dot (e.g., ".txt") or ""

def build_dest_filename(
    base_name: str,
    ts: str,
    out_ext_mode: str,
    src_ext: str
) -> str:
    """
    Construct destination file name:
      - base_name: already without extension
      - out_ext_mode:
          "drop"            -> base_ts
          "keep"            -> base_ts + src_ext
          "force:.csv"      -> base_ts + ".csv" (any .ext works)
    """
    base_ts = f"{base_name}_{ts}"
    mode = out_ext_mode.strip().lower() if out_ext_mode else "drop"
    if mode == "drop":
        return base_ts
    if mode == "keep":
        return f"{base_ts}{src_ext}"
    if mode.startswith("force:"):
        forced_ext = mode.split(":", 1)[1].strip()
        if forced_ext and not forced_ext.startswith("."):
            forced_ext = "." + forced_ext
        return f"{base_ts}{forced_ext}"
    # default safe fallback
    return base_ts

def move_object(
    bucket: str,
    src_key: str,
    dst_key: str,
    *,
    overwrite: bool,
    copy_tags: bool,
    kms_key_id: Optional[str]
) -> Dict:
    if src_key == dst_key:
        raise ValueError("Source and destination keys are identical")

    try:
        head_src = head_object_safe(bucket, src_key)
        if not head_src:
            raise FileNotFoundError(f"Source not found: s3://{bucket}/{src_key}")

        head_dst = head_object_safe(bucket, dst_key)
        if head_dst and not overwrite:
            raise FileExistsError(f"Destination exists: s3://{bucket}/{dst_key}")

        extra = {"MetadataDirective": "COPY"}
        if copy_tags:
            extra["TaggingDirective"] = "COPY"
        if kms_key_id:
            extra["ServerSideEncryption"] = "aws:kms"
            extra["SSEKMSKeyId"] = kms_key_id
        if "ETag" in head_src:
            extra["CopySourceIfMatch"] = head_src["ETag"].strip('"')

        s3.copy_object(Bucket=bucket, Key=dst_key, CopySource={"Bucket": bucket, "Key": src_key}, **extra)
        wait_until_exists(bucket, dst_key)
        s3.delete_object(Bucket=bucket, Key=src_key)
        return {"status": "OK", "src": f"s3://{bucket}/{src_key}", "dst": f"s3://{bucket}/{dst_key}"}
    except (NoCredentialsError, EndpointConnectionError) as e:
        raise S3MoveError(f"Env/credentials/network issue: {e}") from e
    except ClientError as e:
        code = e.response["Error"].get("Code", "Unknown")
        msg = e.response["Error"].get("Message", "")
        raise S3MoveError(f"AWS ClientError {code}: {msg}") from e

# ------------------ Main ---------------------------

def main():
    # Required
    params = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "SRC_KEY", "DST_PREFIX"])
    JOB_NAME   = params["JOB_NAME"]
    BUCKET     = params["BUCKET"]
    SRC_KEY    = params["SRC_KEY"]
    DST_PREFIX = params["DST_PREFIX"].rstrip("/") + "/"

    # Optional
    TZ_OFFSET     = get_arg("TZ_OFFSET", "+05:30")         # IST by default
    TS_FORMAT     = get_arg("TS_FORMAT", "%Y%m%d_%H%M%S")
    OUT_EXT_MODE  = get_arg("OUT_EXT_MODE", "drop")        # drop | keep | force:.csv
    OVERWRITE     = (get_arg("OVERWRITE", "true").lower() == "true")
    COPY_TAGS     = (get_arg("COPY_TAGS", "true").lower() == "true")
    KMS_KEY_ID    = get_arg("KMS_KEY_ID")
    DELETE_SUCCESS = (get_arg("DELETE_SUCCESS", "false").lower() == "true")

    # Glue init
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext); job.init(JOB_NAME, params)

    # List all objects under the source prefix
    folder_prefix = SRC_KEY.rstrip("/") + "/"
    objs = list_objects(BUCKET, folder_prefix)
    if not objs:
        raise FileNotFoundError(f"No objects found under s3://{BUCKET}/{folder_prefix}")

    for obj in objs:
        key = obj["Key"]
        base = os.path.basename(key)
        if key.endswith("/") or base.startswith("_"):
            continue  # Skip folders and housekeeping files

        src_name, src_ext = source_base_from_key(key)
        base_no_ext = src_name
        ts = current_ts(TZ_OFFSET, TS_FORMAT)
        final_filename = build_dest_filename(base_no_ext, ts, OUT_EXT_MODE, src_ext)
        dst_key = f"{DST_PREFIX}{final_filename}"

        print(f"[INFO] Moving s3://{BUCKET}/{key} -> s3://{BUCKET}/{dst_key} (OUT_EXT_MODE={OUT_EXT_MODE})")

        try:
            res = move_object(
                BUCKET,
                key,
                dst_key,
                overwrite=OVERWRITE,
                copy_tags=COPY_TAGS,
                kms_key_id=KMS_KEY_ID
            )
            print(res)
        except Exception as e:
            print(f"[ERROR] Failed to move {key}: {e}")

    # Optional: delete _SUCCESS marker
    if DELETE_SUCCESS:
        success_key = folder_prefix + "_SUCCESS"
        try:
            s3.delete_object(Bucket=BUCKET, Key=success_key)
            print(f"[INFO] Deleted marker: s3://{BUCKET}/{success_key}")
        except s3.exceptions.NoSuchKey:
            pass

    job.commit()


if __name__ == "__main__":
    main()
