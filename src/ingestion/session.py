from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

import settings


def _s3_connector_installed(jars_dir: Path | None = None) -> bool:
    """True if hadoop-aws is already on Spark's classpath (as in the Docker image)."""
    jars_dir = jars_dir or Path(pyspark.__file__).parent / "jars"
    return any(jars_dir.glob("hadoop-aws-*.jar"))


def get_spark(app_name: str = "companies-house", aws_profile: str = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .master("local[*]")                                   # use all local cores, no cluster
        .appName(app_name)
        .config("spark.log.level", "ERROR")
        .config("spark.sql.shuffle.partitions", "8")          # default 200 is absurd for 100 MB
        .config("spark.sql.session.timeZone", "UTC")          # avoid machine-dependent timestamps
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # overwrite only touched partitions
    )
    if settings.uses_s3():
        # Si pasas un perfil, usamos ProfileCredentialsProvider, si no, dejamos el Default
        if aws_profile:
            provider = "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
                .config("spark.hadoop.fs.s3a.profile", aws_profile)
            )
        else:
            provider = "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
            
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", settings.AWS_REGION)
        
        if not _s3_connector_installed():   # on your laptop: download at first start (cached in ~/.ivy2.5.2)
            builder = builder.config("spark.jars.packages",
                                     f"org.apache.hadoop:hadoop-aws:{settings.HADOOP_AWS_VERSION}")
    
    spark = builder.getOrCreate()     
    spark.sparkContext.setLogLevel("ERROR")
        
    return spark




