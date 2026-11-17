"""Shared helpers for the Spark jobs.

All jobs take --root, which can be a local folder (data) or an HDFS URI
(hdfs://namenode:8020/telecom). The layout under the root is the same:

    <root>/raw/...          landing zone (CSV, as received)
    <root>/curated/...      cleaned CDRs + reject table (Parquet)
    <root>/marts/...        star schema + aggregate tables (Parquet)
    <root>/audit/...        run reconciliation + data-quality results (Parquet)
"""
import argparse

from pyspark.sql import SparkSession


def get_spark(app_name, shuffle_partitions=None):
    # No .master() here on purpose: spark-submit decides (local, standalone, YARN).
    # When run with plain `python`, Spark falls back to local[*].
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # bad timestamps -> null, not an exception
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # overwrite only the partitions we write
        .config("spark.sql.adaptive.enabled", "true")
    )
    if shuffle_partitions:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def base_parser(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--root", default="data", help="local folder or hdfs:// URI holding raw/, curated/, marts/, audit/")
    p.add_argument("--shuffle-partitions", type=int, default=None)
    return p


def path(root, *parts):
    return "/".join([root.rstrip("/"), *[p.strip("/") for p in parts]])
