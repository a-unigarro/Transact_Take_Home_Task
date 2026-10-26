"""Storage helpers: the same calls work on a local folder or an S3 location (s3://bucket/prefix)."""
import base64
import hashlib
import os
from pathlib import Path


def is_s3(location) -> bool:
    return isinstance(location, str) and location.startswith("s3://")


def join(base, *parts):
    """Append path parts to a local Path or an s3:// URI."""
    if is_s3(base):
        return "/".join([str(base).rstrip("/"), *(p.strip("/") for p in parts)])
    return Path(base).joinpath(*parts)


def to_spark_path(location) -> str:
    """Spark uses the s3a:// scheme for S3. Local folders stay plain paths."""
    text = str(location)
    return "s3a://" + text[len("s3://"):] if is_s3(location) else text


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def _prefix(uri: str) -> tuple[str, str]:
    """(bucket, key prefix ending in '/', or '' for the bucket root) for listing."""
    bucket, key = _split(uri)
    key = key.strip("/")
    return bucket, (key + "/" if key else "")


def _s3():
    import boto3  # imported here so local-only runs and tests never need AWS libraries
    return boto3.client("s3")


def put_bytes(data: bytes, dest) -> None:
    """Write bytes to dest. Local: atomic via a temp file. S3: a single PUT is atomic."""
    if is_s3(dest):
        bucket, key = _split(dest)
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
        _s3().put_object(Bucket=bucket, Key=key, Body=data, ChecksumSHA256=checksum)
        return
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def read_bytes(location) -> bytes:
    if is_s3(location):
        bucket, key = _split(location)
        return _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    return Path(location).read_bytes()


def list_files(location) -> list[dict]:
    """Files directly inside a location, as [{'name', 'size_bytes'}], sorted by name."""
    if is_s3(location):
        bucket, prefix = _prefix(location)
        files = []
        # The "/" delimiter groups deeper keys into prefixes, so only direct files come back.
        for page in _s3().get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix, Delimiter="/"):
            files += [{"name": o["Key"][len(prefix):], "size_bytes": o["Size"]}
                      for o in page.get("Contents", [])]
        return sorted(files, key=lambda f: f["name"])
    path = Path(location)
    if not path.exists():
        return []
    return sorted(({"name": p.name, "size_bytes": p.stat().st_size} for p in path.iterdir() if p.is_file()),
                  key=lambda f: f["name"])


def delete(location) -> None:
    """Delete one file or object. Deleting something that does not exist is not an error."""
    if is_s3(location):
        bucket, key = _split(location)
        _s3().delete_object(Bucket=bucket, Key=key)    # S3 reports success even if the key is missing
        return
    Path(location).unlink(missing_ok=True)

