"""Performance experiments. Each one answers an interview question with your own numbers.

    python benchmarks/run_benchmarks.py --root data                       # all experiments, local
    spark-submit --master spark://spark-master:7077 /opt/benchmarks/run_benchmarks.py --root hdfs://namenode:8020/telecom

Writes benchmarks/results.csv and benchmarks/results.md. Each timing is the median of --repeats runs
after one warm-up run. Local-mode numbers show the direction of an effect; run on the cluster (and on
more data) for numbers you quote. Always record the machine and row count next to the results.
"""
import argparse
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "jobs", "spark"))

from pyspark.sql import functions as F  # noqa: E402

from common import get_spark, path  # noqa: E402

RESULTS = []


def dir_mb(p):
    if "://" in p:
        return None  # size of HDFS paths: use `hdfs dfs -du -s -h <path>`
    total = 0
    for r, _, fs in os.walk(p):
        total += sum(os.path.getsize(os.path.join(r, f)) for f in fs)
    return round(total / 1e6, 1)


def count_files(p):
    if "://" in p:
        return None
    return sum(len([f for f in fs if f.endswith((".parquet", ".csv", ".orc"))]) for _, _, fs in os.walk(p))


def timed(fn, repeats):
    fn()  # warm-up (JIT, file listing, caches)
    times = []
    for _ in range(repeats):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    return round(statistics.median(times), 2)


def record(experiment, variant, seconds, note="", size_mb=None, files=None):
    RESULTS.append(dict(experiment=experiment, variant=variant, seconds=seconds, size_mb=size_mb, files=files, note=note))
    print(f"[{experiment}] {variant:<34} {seconds:>7}s  size={size_mb}MB files={files}  {note}")


def exp_formats(spark, root, scratch, repeats):
    """CSV vs Parquet vs ORC: storage size and the same aggregation query."""
    cdr = spark.read.parquet(path(root, "curated", "cdr"))
    schema = cdr.schema
    for fmt in ["csv", "parquet", "orc"]:
        out = path(scratch, f"fmt_{fmt}")
        w = cdr.coalesce(4).write.mode("overwrite")
        (w.option("header", True) if fmt == "csv" else w).format(fmt).save(out)

        def q(fmt=fmt, out=out):
            r = spark.read.format(fmt)
            r = r.schema(schema).option("header", True) if fmt == "csv" else r
            r.load(out).where("call_type = 'VOICE'").groupBy("tower_id").agg(F.sum("charge_aed")).collect()

        record("formats", fmt, timed(q, repeats), "filter + group-by on 3 of 12 columns", dir_mb(out), count_files(out))


def exp_pruning(spark, root, scratch, repeats):
    """Partition pruning: filter on the partition column reads one folder instead of all of them."""
    cdr = spark.read.parquet(path(root, "curated", "cdr"))
    day = cdr.agg(F.min("call_date")).first()[0]
    flat, part = path(scratch, "unpartitioned"), path(scratch, "partitioned")
    cdr.write.mode("overwrite").parquet(flat)
    cdr.write.mode("overwrite").partitionBy("call_date").parquet(part)

    for name, p in [("unpartitioned", flat), ("partitioned by call_date", part)]:
        def q(p=p):
            spark.read.parquet(p).where(F.col("call_date") == F.lit(day)).agg(F.count("*"), F.sum("charge_aed")).collect()

        record("pruning", name, timed(q, repeats), f"one-day filter ({day})", dir_mb(p), count_files(p))


def exp_small_files(spark, root, scratch, repeats, n_small):
    """Many tiny files vs few right-sized files: same data, same query."""
    cdr = spark.read.parquet(path(root, "curated", "cdr"))
    for name, n in [(f"{n_small} tiny files", n_small), ("8 compacted files", 8)]:
        out = path(scratch, f"files_{n}")
        cdr.repartition(n).write.mode("overwrite").parquet(out)

        def q(out=out):
            spark.read.parquet(out).groupBy("tower_id").agg(F.count("*"), F.sum("charge_aed")).collect()

        record("small_files", name, timed(q, repeats), "full-scan aggregation", dir_mb(out), count_files(out))


def exp_skew(spark, root, repeats):
    """Joining a skewed fact to a dimension: shuffle join vs AQE skew handling vs broadcast vs salting."""
    fact = spark.read.parquet(path(root, "marts", "fact_usage")).select("tower_sk", "charge_aed")
    dim = spark.read.parquet(path(root, "marts", "dim_tower")).select("tower_sk", "region")
    top = fact.groupBy("tower_sk").count().orderBy(F.desc("count")).first()
    total = fact.count()
    share = round(100 * top["count"] / total, 1)

    def run_join(df):
        df.groupBy("region").agg(F.sum("charge_aed")).collect()

    def conf(**kw):
        for k, v in kw.items():
            spark.conf.set(k.replace("__", "."), v)

    shuffle_conf = dict(spark__sql__autoBroadcastJoinThreshold="-1", spark__sql__shuffle__partitions="16")

    conf(**shuffle_conf, spark__sql__adaptive__enabled="false")
    record("skew", "sort-merge join, AQE off", timed(lambda: run_join(fact.join(dim, "tower_sk")), repeats),
           f"hottest tower = {share}% of rows")

    conf(**shuffle_conf, spark__sql__adaptive__enabled="true", spark__sql__adaptive__skewJoin__enabled="true",
         spark__sql__adaptive__skewJoin__skewedPartitionThresholdInBytes="1MB",
         spark__sql__adaptive__advisoryPartitionSizeInBytes="1MB")
    record("skew", "sort-merge join, AQE skew handling", timed(lambda: run_join(fact.join(dim, "tower_sk")), repeats), "splits oversized partitions")

    conf(spark__sql__autoBroadcastJoinThreshold="10MB")
    record("skew", "broadcast join", timed(lambda: run_join(fact.join(F.broadcast(dim), "tower_sk")), repeats), "small side sent to every executor: no shuffle of the big side")

    conf(**shuffle_conf, spark__sql__adaptive__enabled="false")
    k = 8
    salted_fact = fact.withColumn("salt", (F.rand(seed=7) * k).cast("int"))
    salted_dim = dim.withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(k)])))
    record("skew", f"salted join (K={k}), AQE off", timed(lambda: run_join(salted_fact.join(salted_dim, ["tower_sk", "salt"])), repeats),
           "hot key spread over K partitions; dimension replicated K times")


def write_reports(out_dir, meta):
    import csv

    with open(os.path.join(out_dir, "results.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["experiment", "variant", "seconds", "size_mb", "files", "note"])
        w.writeheader()
        w.writerows(RESULTS)
    lines = [f"# Benchmark results\n", f"{meta}\n", "| experiment | variant | seconds (median) | size MB | files | note |", "|---|---|---|---|---|---|"]
    for r in RESULTS:
        lines.append(f"| {r['experiment']} | {r['variant']} | {r['seconds']} | {r['size_mb'] if r['size_mb'] is not None else ''} | {r['files'] if r['files'] is not None else ''} | {r['note']} |")
    with open(os.path.join(out_dir, "results.md"), "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="data")
    ap.add_argument("--scratch", default=None, help="where experiment copies are written (default: <root>/bench)")
    ap.add_argument("--experiments", default="formats,pruning,small_files,skew")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--small-files", type=int, default=1000)
    ap.add_argument("--report-dir", default=HERE)
    args = ap.parse_args()

    spark = get_spark("telecom-benchmarks", 16)
    scratch = args.scratch or path(args.root, "bench")
    rows = spark.read.parquet(path(args.root, "curated", "cdr")).count()
    wanted = args.experiments.split(",")
    if "formats" in wanted:
        exp_formats(spark, args.root, scratch, args.repeats)
    if "pruning" in wanted:
        exp_pruning(spark, args.root, scratch, args.repeats)
    if "small_files" in wanted:
        exp_small_files(spark, args.root, scratch, args.repeats, args.small_files)
    if "skew" in wanted:
        exp_skew(spark, args.root, args.repeats)
    write_reports(args.report_dir, f"Rows: {rows:,} | master: {spark.sparkContext.master} | cores: {spark.sparkContext.defaultParallelism} | repeats: {args.repeats} (median)")
    spark.stop()


if __name__ == "__main__":
    main()
