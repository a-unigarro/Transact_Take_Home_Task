"""Download the Companies House snapshot, extract the CSV and write its local manifest."""
import csv
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

import settings


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_file(url: str, path: Path):
    if path.exists():
        print(f"File already exists at {path}. Skipping download.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}")
    tmp = path.with_name(path.name + ".part")
    with httpx.stream("GET", url, follow_redirects=True, timeout=60) as response:
        if response.status_code != 200:
            raise Exception(f"Failed to download {url}. Status code: {response.status_code}")
        with open(tmp, "wb") as f:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)
    tmp.rename(path)


def profile_csv(path: Path) -> tuple[list[str], int]:
    """Return (header columns, number of data records).

    The csv module is used instead of counting lines because quoted fields can contain
    commas, quotes and line breaks, so lines != rows.
    """
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        return header, sum(1 for _ in reader)


def download_source(ingestion_date: str | None = None) -> None:
    zip_path = settings.ZIP_DIR / settings.SOURCE_FILE
    download_file(settings.SOURCE_URL, zip_path)

    settings.CSV_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(settings.CSV_DIR)

    path_to_csv = settings.CSV_DIR / f"{settings.SOURCE_NAME}.csv"
    columns, row_count = profile_csv(path_to_csv)

    manifest = {
        "snapshot_date": settings.SNAPSHOT_DATE,
        "csv_path": str(path_to_csv),
        "ingestion_date": ingestion_date or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "row_count": row_count,
        "columns": columns,
        "size_bytes": path_to_csv.stat().st_size,
        "sha256": sha256_of(path_to_csv),
    }

    settings.MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    print(f"Extracted to {settings.CSV_DIR}: {manifest}")


if __name__ == "__main__":
    download_source()