"""Job 2: curated CDRs -> Kimball star schema (1 fact, 4 dimensions, SCD2 customer) + analytics tables.

    spark-submit jobs/spark/build_marts.py --root data
"""
from pyspark.sql import functions as F

from common import base_parser, get_spark, path
from transforms import (
    agg_region_daily, build_dim_customer_scd2, build_dim_date, build_dim_plan, build_dim_tower,
    build_fact_usage, churn_features, tower_health,
)


def run(spark, root):
    def read_csv(name):
        return spark.read.option("header", True).csv(path(root, "raw", name))

    towers, customers = read_csv("towers.csv"), read_csv("customers.csv")
    plans, plan_changes, kpis = read_csv("plans.csv"), read_csv("plan_changes.csv"), read_csv("tower_kpis.csv")
    cdr = spark.read.parquet(path(root, "curated", "cdr"))

    mn, mx = cdr.agg(F.min("call_date"), F.max("call_date")).first()

    dim_date = build_dim_date(spark, mn, mx)
    dim_tower = build_dim_tower(towers)
    dim_plan = build_dim_plan(plans)
    dim_customer = build_dim_customer_scd2(customers, plan_changes, dim_plan)
    fact = build_fact_usage(cdr, dim_customer, dim_tower)

    def write(df, name, partition=None):
        w = df.write.mode("overwrite")
        if partition:
            w = w.partitionBy(partition)
        w.parquet(path(root, "marts", name))

    # Dimensions + fact (the star)
    for name, df in [("dim_date", dim_date), ("dim_tower", dim_tower), ("dim_plan", dim_plan), ("dim_customer", dim_customer)]:
        write(df, name)
    write(fact, "fact_usage", partition="call_date")

    # Analytics tables built from the star (read back, so they use exactly what was written)
    fact_r = spark.read.parquet(path(root, "marts", "fact_usage"))
    dim_tower_r = spark.read.parquet(path(root, "marts", "dim_tower"))
    dim_customer_r = spark.read.parquet(path(root, "marts", "dim_customer"))
    write(agg_region_daily(fact_r, dim_tower_r), "agg_region_daily")
    write(tower_health(kpis), "tower_health")
    write(churn_features(fact_r, dim_customer_r), "churn_features")

    n_fact = fact_r.count()
    print(f"MARTS BUILT: fact_usage={n_fact:,} dim_customer={dim_customer_r.count():,} dim_tower={dim_tower_r.count():,}")
    return n_fact


if __name__ == "__main__":
    args = base_parser(__doc__).parse_args()
    spark = get_spark("telecom-build-marts", args.shuffle_partitions)
    run(spark, args.root)
    spark.stop()
