"""End to end: generate a small dataset, run all three jobs, check results against the generator's ground truth."""
import json
import os
import subprocess
import sys

import pytest
from pyspark.sql import functions as F

import build_marts
import clean_cdr
import dq_checks

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("lake"))
    subprocess.run(
        [sys.executable, os.path.join(ROOT, "generator", "generate_telecom.py"), "--out", out, "--rows", "30000",
         "--customers", "3000", "--towers", "100", "--chunk-rows", "15000"],
        check=True, capture_output=True,
    )
    return out


def test_reconciliation_matches_generator_ground_truth(spark, lake):
    truth = json.load(open(os.path.join(lake, "manifest.json")))
    res = clean_cdr.run(spark, lake)
    assert res["reconciled"]
    assert res["raw"] == truth["rows_raw"]
    assert res["loaded"] == truth["expected_loaded"]
    assert res["rejected"] == truth["expected_rejected"]
    assert res["duplicates_removed"] == truth["expected_duplicates_removed"]
    got = {r["reject_reason"]: r["count"] for r in spark.read.parquet(os.path.join(lake, "audit", "rejects_by_reason")).collect()}
    assert got == {k: v for k, v in truth["rejected_by_reason"].items() if v}


def test_rerun_is_idempotent(spark, lake):
    before = spark.read.parquet(os.path.join(lake, "curated", "cdr")).count()
    clean_cdr.run(spark, lake)
    clean_cdr.run(spark, lake)
    after = spark.read.parquet(os.path.join(lake, "curated", "cdr")).count()
    assert before == after  # re-running the same batch does not add rows


def test_marts_and_dq_checks_pass(spark, lake):
    build_marts.run(spark, lake)
    assert dq_checks.run(spark, lake) is True


def test_anomaly_detection_finds_exactly_the_degraded_towers(spark, lake):
    truth = set(json.load(open(os.path.join(lake, "manifest.json")))["degraded_towers"])
    flagged = {r.tower_id for r in spark.read.parquet(os.path.join(lake, "marts", "tower_health")).where("is_anomalous").collect()}
    assert flagged == truth
