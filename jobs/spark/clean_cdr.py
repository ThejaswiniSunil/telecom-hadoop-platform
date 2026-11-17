"""Job 1: raw CDRs -> validated, de-duplicated, partitioned Parquet + reject table + reconciliation.

    spark-submit jobs/spark/clean_cdr.py --root data
    spark-submit --master spark://spark-master:7077 jobs/spark/clean_cdr.py --root hdfs://namenode:8020/telecom

Reconciliation identity checked on every run:
    raw rows = loaded + rejected + duplicates removed
"""
import datetime as dt
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType, TimestampType,
)

from common import base_parser, get_spark, path
from transforms import RAW_CDR_SCHEMA, deduplicate, flag_cdrs, split_valid_rejected

TOWER_SCHEMA = StructType([StructField(c, StringType()) for c in ["tower_id", "region", "technology", "install_date"]])

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("run_ts", TimestampType()),
        StructField("raw_rows", LongType()),
        StructField("loaded_rows", LongType()),
        StructField("rejected_rows", LongType()),
        StructField("duplicates_removed", LongType()),
        StructField("reconciled", BooleanType()),
    ]
)


def run(spark, root, run_id=None):
    run_id = run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")

    raw = spark.read.option("header", True).schema(RAW_CDR_SCHEMA).csv(path(root, "raw", "cdr"))
    towers = spark.read.option("header", True).schema(TOWER_SCHEMA).csv(path(root, "raw", "towers.csv"))

    raw_rows = raw.count()  # counted BEFORE any join, so a join fan-out would break reconciliation

    flagged = flag_cdrs(raw, towers).persist()
    valid, rejected = split_valid_rejected(flagged)
    curated = deduplicate(valid)

    # Writes. Curated is partitioned by day; dynamic partition overwrite makes re-runs idempotent:
    # re-processing the same input rewrites the same partitions instead of appending duplicates.
    curated.write.mode("overwrite").partitionBy("call_date").parquet(path(root, "curated", "cdr"))
    rejected.withColumn("run_id", F.lit(run_id)).write.mode("overwrite").parquet(path(root, "curated", "cdr_rejected"))

    # Reconcile from what was actually written, not from in-memory counts.
    loaded = spark.read.parquet(path(root, "curated", "cdr")).count()
    rejected_rows = spark.read.parquet(path(root, "curated", "cdr_rejected")).count()
    valid_rows = flagged.where(F.col("reject_reason").isNull()).count()
    duplicates = valid_rows - loaded
    ok = raw_rows == loaded + rejected_rows + duplicates

    by_reason = (
        spark.read.parquet(path(root, "curated", "cdr_rejected")).groupBy("reject_reason").count().orderBy(F.desc("count"))
    )
    by_reason.withColumn("run_id", F.lit(run_id)).write.mode("overwrite").parquet(path(root, "audit", "rejects_by_reason"))

    audit = spark.createDataFrame(
        [(run_id, dt.datetime.now(dt.timezone.utc), raw_rows, loaded, rejected_rows, duplicates, ok)], AUDIT_SCHEMA
    )
    audit.write.mode("append").parquet(path(root, "audit", "run_log"))  # one row per run, never overwritten
    flagged.unpersist()

    result = dict(run_id=run_id, raw=raw_rows, loaded=loaded, rejected=rejected_rows, duplicates_removed=duplicates, reconciled=ok)
    print("RECONCILIATION:", result)
    by_reason.show(truncate=False)
    return result


if __name__ == "__main__":
    args = base_parser(__doc__).parse_args()
    spark = get_spark("telecom-clean-cdr", args.shuffle_partitions)
    res = run(spark, args.root)
    spark.stop()
    sys.exit(0 if res["reconciled"] else 1)
