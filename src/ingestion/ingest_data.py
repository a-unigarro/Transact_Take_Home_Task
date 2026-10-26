"""Raw CSV -> raw Parquet in S3"""

from datetime import datetime, timezone
from pathlib import Path
import json
import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.ingestion import storage
from src.ingestion.session import get_spark

import settings


log = logging.getLogger("ingest_dataset")
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


@dataclass(frozen=True)
class Spec:
    """Everything that differs between datasets. The pipeline code stays generic."""
    schema: StructType
    read_options: dict = field(default_factory=dict)
    lower: tuple = ()      # columns to lowercase
    upper: tuple = ()      # columns to uppercase
    rename: dict = field(default_factory=dict)   # old name -> new name



def read_raw(spark: SparkSession) -> DataFrame:    
    path = (settings.CSV_DIR / settings.SOURCE_NAME).with_suffix('.csv')
    return (spark.read
            .option("header", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "FAILFAST") 
            .csv(storage.to_spark_path(path)))

def manifest_row_count(path: Path | None = None) -> int:
    path_to_manifest = path or settings.MANIFEST_PATH
    entries = json.loads(storage.read_bytes(path_to_manifest))
    return entries["row_count"]

def landing_manifest_location(base_dir=None):
    base_dir = settings.LANDING_DIR if base_dir is None else base_dir
    return storage.join(base_dir, "_manifests", "manifest.json")


def write_landing_manifest(entries: dict, base_dir=None):
    dest = landing_manifest_location(base_dir)
    payload = json.dumps({"snapshot_date": settings.SNAPSHOT_DATE, "datasets": entries}, indent=2)
    storage.put_bytes(payload.encode(), dest)
    return dest



def run(spark: SparkSession | None = None, ingestion_date: str | None = None) -> dict:
    owns_spark = spark is None
    spark = spark or get_spark(f"load-to-s3", aws_profile=settings.AWS_PROFILE)
    try:
        df = read_raw(spark)

        expected, actual = manifest_row_count(), df.count()
        if actual != expected:
            raise ValueError(f"read {actual} rows, manifest says {expected}")
        storage.delete(landing_manifest_location())
        (df.coalesce(1)
           .write.mode("overwrite")
           .parquet(storage.to_spark_path(settings.LANDING_DIR)))
        log.info("wrote %9d rows for ingestion_date=%s", actual, ingestion_date)

        
        entries = {
            "snapshot_date": settings.SNAPSHOT_DATE,
            "ingestion_date": ingestion_date,
            "row_count": actual,
            "columns": [{"name": n, "type": t} for n, t in df.dtypes],
            "files": [{"path": f"snapshot_date={settings.SNAPSHOT_DATE}/{f['name']}",
                        "size_bytes": f["size_bytes"]}
                        for f in storage.list_files(settings.LANDING_DIR)
                        if f["name"].endswith(".parquet")],
        }
        write_landing_manifest(entries)
        written = spark.read.parquet(storage.to_spark_path(settings.LANDING_DIR)).count()
        if written != expected:
            raise ValueError(f"parquet has {written} rows, manifest says {expected}")
        log.info("verified: csv=%d parquet=%d", expected, written)
    finally:
        if owns_spark:
            spark.stop()






if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")    
    date = datetime.now(timezone.utc).date().isoformat()
    run(ingestion_date=date)    

#    parser = argparse.ArgumentParser()
#    parser.add_argument("--dataset", choices=sorted(SPECS), default=None,
#                        help="one dataset; default: all")
#    parser.add_argument("--date", default=None, help="ingestion date; default: latest completed")
#    args = parser.parse_args()
#    run(args.dataset, args.date) if args.dataset else run_all(args.date)