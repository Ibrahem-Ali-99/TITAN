"""Local Spark sessions. The scale path is a different master, not a rewrite."""

import os

from pyspark.sql import SparkSession

from titan_spark.config import Settings


def build_session(settings: Settings, app_name: str) -> SparkSession:
    truststore = os.environ.get("SPARK_S3A_TRUSTSTORE_LOCATION")
    truststore_password = os.environ.get("SPARK_S3A_TRUSTSTORE_PASSWORD")
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "768m")
    )
    if truststore and truststore_password:
        java_options = (
            f"-Djavax.net.ssl.trustStore={truststore} "
            f"-Djavax.net.ssl.trustStorePassword={truststore_password}"
        )
        builder = builder.config("spark.driver.extraJavaOptions", java_options).config(
            "spark.executor.extraJavaOptions", java_options
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _configure_s3(spark, settings)
    return spark


def _configure_s3(spark: SparkSession, settings: Settings) -> None:
    """Apply S3A settings to the live Hadoop configuration.

    ``spark.conf`` values with the ``spark.hadoop`` prefix are copied into
    Hadoop only while the context starts. Setting them afterwards leaves S3A
    on the default credential chain, which does not read the MinIO keys.
    """
    hadoop = spark._jsc.hadoopConfiguration()
    pairs = {
        "fs.s3a.endpoint": settings.s3_endpoint,
        "fs.s3a.path.style.access": "true",
        "fs.s3a.connection.ssl.enabled": os.environ.get("SPARK_S3A_SSL_ENABLED", "false"),
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "fs.s3a.access.key": settings.s3_access_key,
        "fs.s3a.secret.key": settings.s3_secret_key,
        "fs.s3a.endpoint.region": "us-east-1",
        "fs.s3a.change.detection.mode": "none",
    }
    for key, value in pairs.items():
        hadoop.set(key, value)


def copy_tree(spark: SparkSession, source: str, destination: str) -> None:
    """Copy a finished local tree onto the destination.

    The local write is the task commit. The destination, including MinIO, is
    replaced only after that write returns. A retried batch uses the same
    destination and overwrites it. This is not a multi-object transaction.
    """
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    src = jvm.org.apache.hadoop.fs.Path(source)
    dst = jvm.org.apache.hadoop.fs.Path(destination)
    src_fs = src.getFileSystem(conf)
    dst_fs = dst.getFileSystem(conf)
    if dst_fs.exists(dst):
        dst_fs.delete(dst, True)
    copied = jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src, dst_fs, dst, False, True, conf)
    if not copied:
        raise RuntimeError("publishing the dataset tree failed")
