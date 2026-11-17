"""Pure DataFrame transformations (no I/O), so they can be unit-tested on tiny data."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_CDR_COLS = [
    "cdr_id", "caller_id", "callee_number", "tower_id", "call_type",
    "start_ts", "duration_sec", "data_mb", "status", "charge_aed",
]
# Explicit schema: everything is read as string, then parsed and validated by us.
# (No inferSchema: a bad value must become a reject row, not a silent type change or a failed job.)
RAW_CDR_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_CDR_COLS])

VALID_STATUS = ["COMPLETED", "DROPPED", "FAILED"]
VALID_CALL_TYPES = ["VOICE", "SMS", "DATA"]
FAR_FUTURE = "9999-12-31"


def _blank_to_null(c):
    t = F.trim(F.col(c))
    return F.when(t == "", F.lit(None)).otherwise(t).alias(c)


def flag_cdrs(raw: DataFrame, towers: DataFrame) -> DataFrame:
    """Parse, standardise and attach a reject_reason (NULL when the row is valid).

    Rules are applied in a fixed order; the first failing rule names the reason.
    """
    df = raw.select([_blank_to_null(c) for c in RAW_CDR_COLS])
    df = (
        df.withColumn("_start_ts", F.to_timestamp("start_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("_duration", F.col("duration_sec").cast("int"))
        .withColumn("_data_mb", F.col("data_mb").cast("double"))
        .withColumn("_charge", F.col("charge_aed").cast("double"))
        .withColumn("_status", F.upper("status"))
        .withColumn("_call_type", F.upper("call_type"))
    )
    # Reference check: tower must exist. Broadcast join, since the tower table is tiny.
    known = F.broadcast(towers.select(F.col("tower_id").alias("_known_tower")).distinct())
    df = df.join(known, df["tower_id"] == F.col("_known_tower"), "left")

    def not_in(col, values):
        return F.coalesce(F.col(col).isin(*values), F.lit(False)) == F.lit(False)

    reason = (
        F.when(F.col("cdr_id").isNull(), "NULL_CDR_ID")
        .when(F.col("caller_id").isNull(), "NULL_CALLER")
        .when(F.col("_start_ts").isNull(), "BAD_TIMESTAMP")
        .when(F.col("_duration").isNull(), "NULL_DURATION")
        .when(F.col("_duration") < 0, "NEGATIVE_DURATION")
        .when(F.col("_known_tower").isNull(), "UNKNOWN_TOWER")
        .when(not_in("_status", VALID_STATUS), "BAD_STATUS")
        .when(not_in("_call_type", VALID_CALL_TYPES), "BAD_CALL_TYPE")
    )
    return df.withColumn("reject_reason", reason).drop("_known_tower")


def split_valid_rejected(flagged: DataFrame):
    valid = flagged.where(F.col("reject_reason").isNull()).select(
        "cdr_id",
        "caller_id",
        "callee_number",
        "tower_id",
        F.col("_call_type").alias("call_type"),
        F.col("_start_ts").alias("start_ts"),
        F.to_date("_start_ts").alias("call_date"),
        F.hour("_start_ts").alias("call_hour"),
        F.col("_duration").alias("duration_sec"),
        F.coalesce("_data_mb", F.lit(0.0)).alias("data_mb"),
        F.col("_status").alias("status"),
        F.coalesce("_charge", F.lit(0.0)).alias("charge_aed"),
    )
    rejected = flagged.where(F.col("reject_reason").isNotNull()).select(*RAW_CDR_COLS, "reject_reason")
    return valid, rejected


def deduplicate(valid: DataFrame) -> DataFrame:
    # Exact duplicate records share a cdr_id. Keep one.
    return valid.dropDuplicates(["cdr_id"])


# ---------------------------------------------------------------- dimensions

def build_dim_date(spark, min_date, max_date) -> DataFrame:
    d = spark.sql(f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) AS date")
    dow = F.dayofweek("date")  # 1 = Sunday ... 7 = Saturday
    return d.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        (dow.isin(1, 7)).alias("is_weekend"),  # UAE weekend is Saturday-Sunday
    )


def build_dim_tower(towers: DataFrame) -> DataFrame:
    w = Window.orderBy("tower_id")  # small table, single-partition window is fine
    return towers.select(
        F.row_number().over(w).alias("tower_sk"),
        "tower_id",
        "region",
        "technology",
        F.to_date("install_date").alias("install_date"),
    )


def build_dim_plan(plans: DataFrame) -> DataFrame:
    w = Window.orderBy("plan_id")
    return plans.select(
        F.row_number().over(w).alias("plan_sk"),
        "plan_id",
        "plan_name",
        "plan_type",
        F.col("monthly_fee").cast("double").alias("monthly_fee"),
    )


def build_dim_customer_scd2(customers: DataFrame, plan_changes: DataFrame, dim_plan: DataFrame) -> DataFrame:
    """SCD Type 2: one row per customer per plan version, with validity dates.

    valid_from = effective date of the plan, valid_to = day before the next plan starts
    (or 9999-12-31 for the current version).
    """
    w = Window.partitionBy("customer_id").orderBy("effective_date")
    v = (
        plan_changes.withColumn("effective_date", F.to_date("effective_date"))
        .withColumn("next_effective", F.lead("effective_date").over(w))
        .select(
            "customer_id",
            "plan_id",
            F.col("effective_date").alias("valid_from"),
            F.coalesce(F.date_sub("next_effective", 1), F.to_date(F.lit(FAR_FUTURE))).alias("valid_to"),
            F.col("next_effective").isNull().alias("is_current"),
        )
    )
    v = v.join(customers.select("customer_id", "home_region"), "customer_id", "left")
    v = v.join(F.broadcast(dim_plan.select("plan_id", "plan_sk", "plan_name", "plan_type")), "plan_id", "left")
    # Deterministic surrogate key (same input -> same keys on every run).
    # A global ordered window is fine for a dimension; a fact table would never do this.
    sk = Window.orderBy("customer_id", "valid_from")
    return v.select(
        F.row_number().over(sk).alias("customer_sk"),
        "customer_id",
        "home_region",
        "plan_sk",
        "plan_id",
        "plan_name",
        "plan_type",
        "valid_from",
        "valid_to",
        "is_current",
    )


def build_fact_usage(cdr: DataFrame, dim_customer: DataFrame, dim_tower: DataFrame) -> DataFrame:
    """One row per CDR. Each call is joined to the customer version valid on the call date."""
    cust = dim_customer.select(
        F.col("customer_id").alias("_cid"), "customer_sk", "plan_sk", "valid_from", "valid_to"
    )
    cond = (cdr["caller_id"] == cust["_cid"]) & (cdr["call_date"] >= cust["valid_from"]) & (cdr["call_date"] <= cust["valid_to"])
    f = cdr.join(cust, cond, "left")
    f = f.join(F.broadcast(dim_tower.select("tower_id", "tower_sk")), "tower_id", "left")
    return f.select(
        "cdr_id",
        F.date_format("call_date", "yyyyMMdd").cast("int").alias("date_key"),
        "call_hour",
        F.coalesce("customer_sk", F.lit(-1)).alias("customer_sk"),  # -1 = unknown member (checked by DQ)
        F.coalesce("tower_sk", F.lit(-1)).alias("tower_sk"),
        F.coalesce("plan_sk", F.lit(-1)).alias("plan_sk"),
        "call_type",
        "status",
        (F.col("status") == "DROPPED").alias("is_dropped"),
        "duration_sec",
        "data_mb",
        "charge_aed",
        "call_date",
    )


# ---------------------------------------------------------------- analytics

def agg_region_daily(fact: DataFrame, dim_tower: DataFrame) -> DataFrame:
    return (
        fact.join(F.broadcast(dim_tower.select("tower_sk", "region")), "tower_sk", "left")
        .groupBy("region", "call_date")
        .agg(
            F.count("*").alias("records"),
            F.round(F.avg(F.col("is_dropped").cast("int")), 4).alias("drop_rate"),
            F.round(F.sum("charge_aed"), 2).alias("revenue_aed"),
            F.round(F.sum("data_mb") / 1024.0, 2).alias("data_gb"),
            F.countDistinct("customer_sk").alias("active_customers"),
        )
    )


def tower_health(kpis: DataFrame, ratio: float = 2.0, min_hours: int = 6) -> DataFrame:
    """Flag towers whose latency runs at >= `ratio` x their own median for >= `min_hours` hours."""
    k = kpis.select(
        "tower_id",
        F.to_timestamp("hour_ts", "yyyy-MM-dd HH:mm:ss").alias("hour_ts"),
        F.col("latency_ms").cast("double").alias("latency_ms"),
        F.col("drop_rate").cast("double").alias("drop_rate"),
    )
    med = k.groupBy("tower_id").agg(F.percentile_approx("latency_ms", 0.5).alias("median_latency_ms"))
    j = k.join(med, "tower_id").withColumn(
        "degraded_hour", F.col("latency_ms") >= ratio * F.col("median_latency_ms")
    )
    return (
        j.groupBy("tower_id", "median_latency_ms")
        .agg(
            F.sum(F.col("degraded_hour").cast("int")).alias("degraded_hours"),
            F.round(F.avg("drop_rate"), 4).alias("avg_drop_rate"),
            F.max("latency_ms").alias("max_latency_ms"),
        )
        .withColumn("is_anomalous", F.col("degraded_hours") >= min_hours)
    )


def churn_features(fact: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Usage-decline feature table (a plain feature set, not a model).

    Compares the last 7 days of activity with the 7 days before.
    """
    max_d = fact.agg(F.max("call_date")).first()[0]
    last7_start = F.date_sub(F.lit(max_d), 6)
    prev7_start = F.date_sub(F.lit(max_d), 13)
    c = fact.join(dim_customer.select("customer_sk", "customer_id"), "customer_sk")
    agg = c.groupBy("customer_id").agg(
        F.sum(F.when(F.col("call_date") >= last7_start, 1).otherwise(0)).alias("records_last7"),
        F.sum(F.when((F.col("call_date") >= prev7_start) & (F.col("call_date") < last7_start), 1).otherwise(0)).alias("records_prev7"),
        F.sum(F.when(F.col("call_date") >= last7_start, F.col("charge_aed")).otherwise(0.0)).alias("revenue_last7"),
    )
    return agg.withColumn(
        "usage_change_pct",
        F.round((F.col("records_last7") - F.col("records_prev7")) / F.col("records_prev7") * 100, 1),
    ).withColumn("usage_decline_flag", (F.col("records_prev7") >= 5) & (F.col("usage_change_pct") <= -50))
