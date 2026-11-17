"""Unit tests on tiny hand-written data: every rule and the SCD2 logic, with known answers."""
import datetime as dt

from pyspark.sql import functions as F

from transforms import (
    RAW_CDR_COLS, build_dim_customer_scd2, build_dim_plan, build_dim_tower, build_fact_usage,
    deduplicate, flag_cdrs, split_valid_rejected, tower_health,
)


def raw_df(spark, rows):
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in RAW_CDR_COLS))


def towers_df(spark):
    return spark.createDataFrame([("T1", "Dubai", "5G", "2024-01-01")], ["tower_id", "region", "technology", "install_date"])


GOOD = ("C1", "CU1", "971500000001", "T1", "VOICE", "2026-09-01 10:00:00", "60", "0.0", "COMPLETED", "0.25")


def reasons(spark, row):
    out = flag_cdrs(raw_df(spark, [row]), towers_df(spark)).select("reject_reason").first()[0]
    return out


def test_good_row_is_valid(spark):
    assert reasons(spark, GOOD) is None


def test_each_reject_rule(spark):
    def mod(idx, val):
        r = list(GOOD)
        r[idx] = val
        return tuple(r)

    assert reasons(spark, mod(1, None)) == "NULL_CALLER"
    assert reasons(spark, mod(1, "   ")) == "NULL_CALLER"  # blank counts as null
    assert reasons(spark, mod(5, "not_a_date")) == "BAD_TIMESTAMP"
    assert reasons(spark, mod(5, "2026-13-45 99:99:99")) == "BAD_TIMESTAMP"
    assert reasons(spark, mod(6, None)) == "NULL_DURATION"
    assert reasons(spark, mod(6, "-5")) == "NEGATIVE_DURATION"
    assert reasons(spark, mod(3, "T9999")) == "UNKNOWN_TOWER"
    assert reasons(spark, mod(8, "UNKNOWN")) == "BAD_STATUS"
    assert reasons(spark, mod(8, None)) == "BAD_STATUS"


def test_status_variants_are_cleaned_not_rejected(spark):
    rows = [GOOD[:8] + (" completed ",) + GOOD[9:], GOOD[:8] + ("Dropped",) + GOOD[9:]]
    valid, rejected = split_valid_rejected(flag_cdrs(raw_df(spark, [("C%d" % i,) + r[1:] for i, r in enumerate(rows)]), towers_df(spark)))
    assert rejected.count() == 0
    assert sorted(r[0] for r in valid.select("status").collect()) == ["COMPLETED", "DROPPED"]


def test_dedup_keeps_one_row_per_cdr_id(spark):
    flagged = flag_cdrs(raw_df(spark, [GOOD, GOOD, ("C2",) + GOOD[1:]]), towers_df(spark))
    valid, _ = split_valid_rejected(flagged)
    assert deduplicate(valid).count() == 2


def test_no_row_is_lost_or_multiplied(spark):
    rows = [GOOD, GOOD, ("C2", None) + GOOD[2:], ("C3",) + GOOD[1:3] + ("T9999",) + GOOD[4:]]
    raw = raw_df(spark, rows)
    flagged = flag_cdrs(raw, towers_df(spark))
    valid, rejected = split_valid_rejected(flagged)
    assert raw.count() == valid.count() + rejected.count()


def _dims(spark):
    customers = spark.createDataFrame([("CU1", "Dubai", "2025-01-01")], ["customer_id", "home_region", "activation_date"])
    plans = spark.createDataFrame([("P1", "Lite", "PREPAID", 25.0), ("P2", "Max", "POSTPAID", 199.0)], ["plan_id", "plan_name", "plan_type", "monthly_fee"])
    changes = spark.createDataFrame(
        [("CU1", "P1", "2025-01-01"), ("CU1", "P2", "2026-09-16")], ["customer_id", "plan_id", "effective_date"]
    )
    dim_plan = build_dim_plan(plans)
    return build_dim_customer_scd2(customers, changes, dim_plan), dim_plan


def test_scd2_intervals(spark):
    dim, _ = _dims(spark)
    rows = {r.plan_id: r for r in dim.collect()}
    assert rows["P1"].valid_from == dt.date(2025, 1, 1)
    assert rows["P1"].valid_to == dt.date(2026, 9, 15)  # day before the next version starts
    assert rows["P1"].is_current is False
    assert rows["P2"].valid_to == dt.date(9999, 12, 31)
    assert rows["P2"].is_current is True


def test_fact_joins_to_the_plan_valid_on_the_call_date(spark):
    dim_c, _ = _dims(spark)
    dim_t = build_dim_tower(towers_df(spark))
    cdr = spark.createDataFrame(
        [
            ("A", "CU1", "T1", "VOICE", dt.datetime(2026, 9, 10, 9), dt.date(2026, 9, 10), 9, 60, 0.0, "COMPLETED", 0.25),
            ("B", "CU1", "T1", "VOICE", dt.datetime(2026, 9, 20, 9), dt.date(2026, 9, 20), 9, 60, 0.0, "COMPLETED", 0.25),
        ],
        "cdr_id string, caller_id string, tower_id string, call_type string, start_ts timestamp, call_date date, call_hour int, duration_sec int, data_mb double, status string, charge_aed double",
    )
    fact = build_fact_usage(cdr, dim_c, dim_t)
    plan_of = {r.cdr_id: r.plan_sk for r in fact.collect()}
    sk = {r.plan_id: r.plan_sk for r in dim_c.collect()}
    assert plan_of["A"] == sk["P1"]  # before the change
    assert plan_of["B"] == sk["P2"]  # after the change: history is preserved, not overwritten


def test_tower_health_flags_degraded_tower(spark):
    rows = []
    for h in range(48):
        rows.append(("TOK", f"2026-09-01 {h % 24:02d}:00:00", "0.01", "40", "60"))
        lat = "130" if h >= 36 else "40"  # 12 degraded hours
        rows.append(("TBAD", f"2026-09-01 {h % 24:02d}:00:00", "0.05", lat, "20"))
    kpis = spark.createDataFrame(rows, ["tower_id", "hour_ts", "drop_rate", "latency_ms", "throughput_mbps"])
    flags = {r.tower_id: r.is_anomalous for r in tower_health(kpis).collect()}
    assert flags == {"TOK": False, "TBAD": True}
