import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _location(env_var: str, default: Path) -> "str | Path":
    """A data location from the environment: an s3:// URI stays a string, anything else is a Path."""
    value = os.getenv(env_var)
    if not value:
        return default
    return value if value.startswith("s3://") else Path(value)


PROJECT_DIR = Path(__file__).resolve().parent

SOURCE_NAME = os.environ["SOURCE_NAME"]
SOURCE_FILE = Path(f"{SOURCE_NAME}.zip")
SNAPSHOT_DATE = re.search(r"\d{4}-\d{2}-\d{2}", SOURCE_FILE.name).group()
SOURCE_URL = f"https://download.companieshouse.gov.uk/{SOURCE_FILE.name}"

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
HADOOP_AWS_VERSION = os.getenv("HADOOP_AWS_VERSION", "3.5.0")

DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_DIR / "data"))

COUNTRY_DIR = DATA_DIR / "COUNTRY=UK"

ZIP_DIR = COUNTRY_DIR / "zip"
CSV_DIR = COUNTRY_DIR / "csv" / f"snapshot_date={SNAPSHOT_DATE}"
MANIFEST_PATH = CSV_DIR / "manifest.json"

LANDING_ROOT = _location("LANDING_ROOT", COUNTRY_DIR / "landing")

if isinstance(LANDING_ROOT, Path):
    LANDING_DIR = LANDING_ROOT / f"snapshot_date={SNAPSHOT_DATE}"
else:
    LANDING_DIR = f"{LANDING_ROOT}/snapshot_date={SNAPSHOT_DATE}"
def uses_s3() -> bool:
    return isinstance(LANDING_DIR, str) and LANDING_DIR.startswith("s3://")


JDBC_JAR_NAME = "postgresql-42.7.4.jar"
JDBC_JAR_URL = f"https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/{JDBC_JAR_NAME}"


