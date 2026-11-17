"""Job 3: automated data-quality checks. Writes results to <root>/audit/dq_results and exits 1 if any check fails.

    spark-submit jobs/spark/dq_checks.py --root data
"""
import datetime as dt
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from common import base_parser, get_spark, path
from transforms import VALID_CALL_TYPES, VALID_STATUS

SCHEMA = StructType(
    [
        StructField("check_name", StringType()),
        StructField("passed", BooleanType()),
        StructField("detail", StringType()),
        StructField("checked_at", TimestampType()),
    ]
)


def run(spark, root):
    cdr = spark.read.parquet(path(root, "curated", "cdr"))
    fact = spark.read.parquet(path(root, "marts", "fact_usage"))
    dim_c = spark.read.parquet(path(root, "marts", "dim_customer"))
    run_log = spark.read.parquet(path(root, "audit", "run_log")).orderBy(F.desc("run_ts")).limit(1).first()

    results = []

    def check(name, passed, detail):
        results.append((name, bool(passed), str(detail), dt.datetime.now(dt.timezone.utc)))

    n_cdr = cdr.count()

    # 1-4: curated layer
    dupes = n_cdr - cdr.select("cdr_id").distinct().count()
    check("curated: cdr_id is unique", dupes == 0, f"duplicate ids={dupes}")

    key_cols = ["cdr_id", "caller_id", "tower_id", "start_ts", "call_type", "status"]
    nulls = cdr.where(" OR ".join(f"{c} IS NULL" for c in key_cols)).count()
    check("curated: no nulls in key columns", nulls == 0, f"rows with nulls={nulls}")

    neg = cdr.where(F.col("duration_sec") < 0).count()
    check("curated: duration >= 0", neg == 0, f"negative durations={neg}")

    bad_dom = cdr.where(~F.col("status").isin(*VALID_STATUS) | ~F.col("call_type").isin(*VALID_CALL_TYPES)).count()
    check("curated: status / call_type in allowed values", bad_dom == 0, f"rows outside domain={bad_dom}")

    # 5-8: fact layer
    n_fact = fact.count()
    check("fact: row count equals curated", n_fact == n_cdr, f"fact={n_fact:,} curated={n_cdr:,}")

    orphan_c = fact.where(F.col("customer_sk") == -1).count()
    check("fact: every call resolves to a customer version", orphan_c == 0, f"unresolved customers={orphan_c}")

    orphan_t = fact.where(F.col("tower_sk") == -1).count()
    check("fact: every call resolves to a tower", orphan_t == 0, f"unresolved towers={orphan_t}")

    a = cdr.agg(F.round(F.sum("charge_aed"), 2)).first()[0]
    b = fact.agg(F.round(F.sum("charge_aed"), 2)).first()[0]
    check("fact: revenue control total matches curated", a == b, f"curated={a} fact={b}")

    # 9-10: SCD2 integrity
    cur = dim_c.where("is_current").groupBy("customer_id").count().where("count != 1").count()
    check("dim_customer: exactly one current row per customer", cur == 0, f"customers violating={cur}")

    w = Window.partitionBy("customer_id").orderBy("valid_from")
    gaps = (
        dim_c.withColumn("prev_to", F.lag("valid_to").over(w))
        .where(F.col("prev_to").isNotNull() & (F.date_add("prev_to", 1) != F.col("valid_from")))
        .count()
    )
    check("dim_customer: version intervals contiguous, no overlap", gaps == 0, f"gaps/overlaps={gaps}")

    # 11-12: reconciliation from the last pipeline run
    r = run_log
    check(
        "audit: raw = loaded + rejected + duplicates",
        r["raw_rows"] == r["loaded_rows"] + r["rejected_rows"] + r["duplicates_removed"],
        f"raw={r['raw_rows']:,} loaded={r['loaded_rows']:,} rejected={r['rejected_rows']:,} dup={r['duplicates_removed']:,}",
    )
    check("audit: loaded rows equal curated rows", r["loaded_rows"] == n_cdr, f"run_log={r['loaded_rows']:,} curated={n_cdr:,}")

    df = spark.createDataFrame(results, SCHEMA)
    df.write.mode("append").parquet(path(root, "audit", "dq_results"))
    for name, ok, detail, _ in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  ({detail})")
    failed = [x for x in results if not x[1]]
    print(f"DQ SUMMARY: {len(results) - len(failed)}/{len(results)} passed")
    return len(failed) == 0


if __name__ == "__main__":
    args = base_parser(__doc__).parse_args()
    spark = get_spark("telecom-dq-checks", args.shuffle_partitions)
    ok = run(spark, args.root)
    spark.stop()
    sys.exit(0 if ok else 1)
