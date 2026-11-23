"""Factory for a lightweight SparkSession configured for Hudi 0.15.0 and MinIO S3A."""

from pyspark.sql import SparkSession

from config import MINIO_ENDPOINT, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER


def get_spark(app_name: str) -> SparkSession:
    """Create or return a local SparkSession with Kryo, Hudi, and MinIO S3A settings."""
    use_ssl = "true" if MINIO_ENDPOINT.startswith("https://") else "false"

    return (
        SparkSession.builder.appName(app_name)
        # Hudi requires Kryo serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.kryo.registrator",
            "org.apache.spark.HoodieSparkKryoRegistrar",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        # MinIO S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", use_ssl)
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Keep memory footprint and task overhead small on a 3.7 GiB machine
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
