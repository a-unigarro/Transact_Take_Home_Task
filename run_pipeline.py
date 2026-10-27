"""Run the whole pipeline: JDBC driver -> Postgres -> download -> Parquet -> Glue job.

Usage:
    python run_pipeline.py                  # all steps, one job run
    python run_pipeline.py --runs 2         # run the job twice to show idempotency
    python run_pipeline.py --skip-download --skip-ingest   # only the Glue job
"""
import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import settings

ROOT = settings.PROJECT_DIR
CONTAINER_WORKSPACE = "/home/hadoop/workspace"
# Lines of the (very verbose) Spark log worth showing on screen; the full log goes to a file.
KEY_LINES = re.compile(r"read=|rows inserted|DQ report|WARNING|Traceback|Error|Exception")


def step(title: str) -> None:
    print(f"\n=== {title} ===", flush=True)


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def landing_path() -> str:
    """Parquet location as the Glue container sees it."""
    if settings.uses_s3():
        return str(settings.LANDING_DIR)
    try:
        rel = Path(settings.LANDING_DIR).resolve().relative_to(ROOT)
    except ValueError:
        sys.exit("Local landing folder must be inside the project (it is mounted into the container).")
    return f"{CONTAINER_WORKSPACE}/{rel.as_posix()}"


def prepare_output_dir() -> None:
    """The container user (UID 10000) must be able to write the DQ report."""
    out = ROOT / "output" / "dq"
    out.mkdir(parents=True, exist_ok=True)
    try:
        for p in (ROOT / "output", out):
            p.chmod(0o777)
    except PermissionError:
        print("WARNING: could not chmod output/ (owned by another user?); run: sudo chmod -R 777 output")


def run_glue_job(run_number: int, source_path: str) -> None:
    log_dir = ROOT / "output" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"glue_{settings.SNAPSHOT_DATE}_run{run_number}.log"
    cmd = ["docker", "compose", "run", "--rm", "-T", "glue", "spark-submit",
           "--jars", f"{CONTAINER_WORKSPACE}/jars/{settings.JDBC_JAR_NAME}",
           "src/glue/transform_load.py", "--SOURCE_PATH", source_path]
    with subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, errors="replace") as proc, log_file.open("w") as log:
        for line in proc.stdout:
            log.write(line)
            if KEY_LINES.search(line):
                print("  " + line.rstrip(), flush=True)
    if proc.returncode != 0:
        sys.exit(f"Glue job failed (exit {proc.returncode}). Full log: {log_file}")
    print(f"  full log: {log_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-download", action="store_true", help="CSV and manifest already exist")
    parser.add_argument("--skip-ingest", action="store_true", help="Parquet already exists")
    parser.add_argument("--runs", type=int, default=1, help="how many times to run the Glue job")
    args = parser.parse_args()

    step("1/5 JDBC driver")
    run([sys.executable, "-m", "scripts.get_jdbc"])

    step("2/5 PostgreSQL")
    prepare_output_dir()
    run(["docker", "compose", "up", "-d", "--wait", "db"])

    if not args.skip_download:
        step("3/5 Download CSV")
        from src.download_source import download_source   # imported here so skipped steps need no deps
        download_source()

    if not args.skip_ingest:
        step("4/5 CSV -> Parquet")
        from src.ingestion.ingest_data import run as ingest
        ingest(ingestion_date=datetime.now(timezone.utc).date().isoformat())

    source_path = landing_path()
    for i in range(1, args.runs + 1):
        step(f"5/5 Glue job (run {i}/{args.runs}) <- {source_path}")
        run_glue_job(i, source_path)

    print("\nDone. DQ report: output/dq/snapshot_date=%s/report.json" % settings.SNAPSHOT_DATE)


if __name__ == "__main__":
    main()