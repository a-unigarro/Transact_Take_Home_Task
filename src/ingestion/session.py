from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

import settings


def _s3_connector_installed(jars_dir: Path | None = None) -> bool:
    """True if hadoop-aws is already on Spark's classpath (as in the Docker image)."""
    jars_dir = jars_dir or Path(pyspark.__file__).parent / "jars"
    return any(jars_dir.glob("hadoop-aws-*.jar"))


def get_spark(app_name: str = "companies-house") -> SparkSession:
    builder = (
        SparkSession.builder
        .master("local[*]")                                   # use all local cores, no cluster
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")          # the default of 200 is far too many for this volume
        .config("spark.sql.session.timeZone", "UTC")          # avoid machine-dependent timestamps
    )
    if settings.uses_s3():
        # Default provider chain: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from .env (exported by load_dotenv)
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.AWS_REGION)
        )
        if not _s3_connector_installed():   # on a laptop the connector is downloaded at first start
            builder = builder.config("spark.jars.packages",
                                     f"org.apache.hadoop:hadoop-aws:{settings.HADOOP_AWS_VERSION}")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark