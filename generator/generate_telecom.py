#!/usr/bin/env python3
"""Synthetic telecom data generator.

100% synthetic. No real operator data, schemas or identifiers are used.
The data is shaped like typical telecom usage records so the pipeline has
realistic problems to solve:

* CDRs (call/SMS/data records) with deliberately injected defects
* heavy tower skew (a few towers carry most of the traffic)
* tower KPI time series, with a few towers degraded on purpose
* customers whose plan changes over time (for SCD Type 2)

A manifest.json with the ground truth (what was injected, what the pipeline
should reject, which towers are degraded) is written next to the data, so the
pipeline can be verified against known answers.

Usage:
    python generator/generate_telecom.py --out data --rows 1000000
"""
import argparse
import json
import os
import shutil

import numpy as np
import pandas as pd

REGIONS = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Fujairah", "Umm Al Quwain"]
REGION_P = [0.34, 0.28, 0.17, 0.06, 0.07, 0.05, 0.03]

PLANS = pd.DataFrame(
    {
        "plan_id": ["P1", "P2", "P3", "P4", "P5", "P6"],
        "plan_name": ["Prepaid Lite", "Prepaid Plus", "Postpaid Basic", "Postpaid Max", "Family Share", "Business 5G"],
        "plan_type": ["PREPAID", "PREPAID", "POSTPAID", "POSTPAID", "POSTPAID", "POSTPAID"],
        "monthly_fee": [25.0, 60.0, 99.0, 199.0, 249.0, 349.0],
    }
)

# Defect rates, applied to disjoint sets of rows (one defect per row at most).
# The first six are rejected by the pipeline; STATUS_VARIANT is cleaned, not rejected.
DEFECT_RATES = {
    "NULL_CALLER": 0.004,
    "BAD_TIMESTAMP": 0.005,
    "NULL_DURATION": 0.003,
    "NEGATIVE_DURATION": 0.004,
    "UNKNOWN_TOWER": 0.003,
    "BAD_STATUS": 0.002,
    "STATUS_VARIANT": 0.02,
}
REJECT_REASONS = {k for k in DEFECT_RATES if k != "STATUS_VARIANT"}
HOUR_WEIGHTS = np.array(
    [1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 6, 6, 7, 6, 6, 6, 7, 8, 9, 9, 8, 6, 4, 2], dtype=float
)
HOUR_P = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()


def make_reference(rng, n_towers, n_customers, start, days):
    tower_ids = np.array([f"T{i:04d}" for i in range(1, n_towers + 1)])
    towers = pd.DataFrame(
        {
            "tower_id": tower_ids,
            "region": rng.choice(REGIONS, size=n_towers, p=REGION_P),
            "technology": rng.choice(["4G", "5G"], size=n_towers, p=[0.6, 0.4]),
            "install_date": (start - pd.to_timedelta(rng.integers(200, 2000, n_towers), unit="D")).strftime("%Y-%m-%d"),
        }
    )

    cust_ids = np.array([f"CU{i:07d}" for i in range(1, n_customers + 1)])
    customers = pd.DataFrame(
        {
            "customer_id": cust_ids,
            "home_region": rng.choice(REGIONS, size=n_customers, p=REGION_P),
            "activation_date": (start - pd.to_timedelta(rng.integers(30, 1500, n_customers), unit="D")).strftime("%Y-%m-%d"),
        }
    )

    # Plan history: every customer has an initial plan; ~25% change plan once during the window.
    plan_ids = PLANS["plan_id"].to_numpy()
    first_idx = rng.integers(0, len(plan_ids), n_customers)
    initial = pd.DataFrame(
        {"customer_id": cust_ids, "plan_id": plan_ids[first_idx], "effective_date": customers["activation_date"].to_numpy()}
    )
    changers = rng.random(n_customers) < 0.25
    n_ch = int(changers.sum())
    new_idx = (first_idx[changers] + rng.integers(1, len(plan_ids), n_ch)) % len(plan_ids)
    changed = pd.DataFrame(
        {
            "customer_id": cust_ids[changers],
            "plan_id": plan_ids[new_idx],
            "effective_date": (start + pd.to_timedelta(rng.integers(1, max(days - 1, 2), n_ch), unit="D")).strftime("%Y-%m-%d"),
        }
    )
    plan_changes = pd.concat([initial, changed], ignore_index=True)
    return towers, customers, plan_changes


def make_kpis(rng, towers, start, days):
    """Hourly KPIs per tower. ~2% of towers degrade (latency x3, drops x4) in the last 3 days."""
    n_t = len(towers)
    hours = days * 24
    n_deg = max(1, int(round(0.02 * n_t)))
    degraded = rng.choice(towers["tower_id"].to_numpy(), size=n_deg, replace=False)

    tower_col = np.repeat(towers["tower_id"].to_numpy(), hours)
    hour_idx = np.tile(np.arange(hours), n_t)
    ts = np.datetime64(start.strftime("%Y-%m-%dT%H:%M:%S")) + (hour_idx * 3600).astype("timedelta64[s]")
    ts_str = np.char.replace(np.datetime_as_string(ts, unit="s"), "T", " ")

    base_lat = np.repeat(rng.normal(40, 6, n_t).clip(20, 70), hours)
    base_drop = np.repeat(rng.uniform(0.008, 0.03, n_t), hours)
    base_thr = np.repeat(rng.normal(60, 10, n_t).clip(20, 120), hours)
    latency = base_lat + rng.normal(0, 3, n_t * hours)
    drop = base_drop + rng.normal(0, 0.003, n_t * hours)
    thr = base_thr + rng.normal(0, 4, n_t * hours)

    is_deg = np.isin(tower_col, degraded) & (hour_idx >= (days - 3) * 24)
    latency = np.where(is_deg, latency * 3.0, latency)
    drop = np.where(is_deg, drop * 4.0, drop)
    thr = np.where(is_deg, thr * 0.4, thr)

    kpis = pd.DataFrame(
        {
            "tower_id": tower_col,
            "hour_ts": ts_str,
            "drop_rate": np.round(np.clip(drop, 0.0, 1.0), 4),
            "latency_ms": np.round(np.clip(latency, 1, None), 1),
            "throughput_mbps": np.round(np.clip(thr, 1, None), 1),
        }
    )
    return kpis, sorted(degraded.tolist())


def gen_cdr_chunk(rng, n, offset, ctx, dup_rate):
    n_cust = ctx["n_customers"]
    ids = np.arange(offset, offset + n)
    cdr_id = np.char.add("C", np.char.zfill(ids.astype(str), 12))
    caller = np.char.add("CU", np.char.zfill((rng.integers(0, n_cust, n) + 1).astype(str), 7))
    callee = np.char.add("9715", np.char.zfill(rng.integers(0, 10**8, n).astype(str), 8))
    tower = rng.choice(ctx["tower_ids"], size=n, p=ctx["tower_p"])
    call_type = rng.choice(["VOICE", "SMS", "DATA"], size=n, p=[0.55, 0.20, 0.25])

    day = rng.integers(0, ctx["days"], n)
    hour = rng.choice(24, size=n, p=HOUR_P)
    sec = rng.integers(0, 3600, n)
    base = np.datetime64(ctx["start_str"])
    ts = base + (day * 86400 + hour * 3600 + sec).astype("timedelta64[s]")
    ts_str = np.char.replace(np.datetime_as_string(ts, unit="s"), "T", " ")

    voice_dur = np.clip(rng.lognormal(4.5, 1.0, n), 1, 3600).astype(int)
    data_dur = np.clip(rng.lognormal(5.5, 1.0, n), 10, 7200).astype(int)
    duration = np.where(call_type == "VOICE", voice_dur, np.where(call_type == "DATA", data_dur, 0))
    data_mb = np.where(call_type == "DATA", np.round(rng.lognormal(3.0, 1.2, n), 2), 0.0)
    status = rng.choice(["COMPLETED", "DROPPED", "FAILED"], size=n, p=[0.93, 0.04, 0.03])
    charge = np.where(
        call_type == "VOICE", duration / 60 * 0.25, np.where(call_type == "SMS", 0.10, data_mb * 0.02)
    )

    df = pd.DataFrame(
        {
            "cdr_id": cdr_id,
            "caller_id": caller,
            "callee_number": callee,
            "tower_id": tower,
            "call_type": call_type,
            "start_ts": ts_str,
            "duration_sec": duration.astype(str),
            "data_mb": data_mb.astype(str),
            "status": status,
            "charge_aed": np.round(charge, 2).astype(str),
        }
    )
    df = df.astype(object)

    # ---- inject defects on disjoint row sets ----
    label = np.array(["ok"] * n, dtype=object)
    u = rng.random(n)
    lo = 0.0
    for name, rate in DEFECT_RATES.items():
        hi = lo + rate
        m = (u >= lo) & (u < hi)
        k = int(m.sum())
        lo = hi
        if k == 0:
            continue
        label[m] = name
        if name == "NULL_CALLER":
            df.loc[m, "caller_id"] = None
        elif name == "BAD_TIMESTAMP":
            df.loc[m, "start_ts"] = rng.choice(["N/A", "2026-13-45 99:99:99", "not_a_date", ""], size=k)
        elif name == "NULL_DURATION":
            df.loc[m, "duration_sec"] = None
        elif name == "NEGATIVE_DURATION":
            df.loc[m, "duration_sec"] = np.char.add("-", rng.integers(1, 500, k).astype(str)).astype(object)
        elif name == "UNKNOWN_TOWER":
            df.loc[m, "tower_id"] = "T9999"
        elif name == "BAD_STATUS":
            df.loc[m, "status"] = rng.choice(["UNKNOWN", "??", "N/A", ""], size=k)
        elif name == "STATUS_VARIANT":
            cur = df.loc[m, "status"].to_numpy()
            pick = rng.random(k) < 0.5
            df.loc[m, "status"] = [s.lower() if p else f" {s} " for s, p in zip(cur, pick)]

    # ---- exact duplicates (copies of rows, including some defective ones) ----
    n_dup = int(n * dup_rate)
    dup_idx = rng.choice(n, size=n_dup, replace=False)
    dups = df.iloc[dup_idx]
    all_df = pd.concat([df, dups], ignore_index=True)
    all_label = np.concatenate([label, label[dup_idx]])
    is_dup = np.concatenate([np.zeros(n, bool), np.ones(n_dup, bool)])

    perm = rng.permutation(len(all_df))
    all_df = all_df.iloc[perm].reset_index(drop=True)
    all_label, is_dup = all_label[perm], is_dup[perm]

    stats = {
        "rows_raw": int(len(all_df)),
        "expected_rejected": int(np.isin(all_label, list(REJECT_REASONS)).sum()),
        "expected_duplicates_removed": int((is_dup & ~np.isin(all_label, list(REJECT_REASONS))).sum()),
        "expected_loaded": int((~is_dup & ~np.isin(all_label, list(REJECT_REASONS))).sum()),
        "rejected_by_reason": {r: int((all_label == r).sum()) for r in REJECT_REASONS},
    }
    return all_df, stats


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data", help="output root; files go to <out>/raw/...")
    ap.add_argument("--rows", type=int, default=1_000_000, help="base CDR rows (duplicates are added on top)")
    ap.add_argument("--towers", type=int, default=500)
    ap.add_argument("--customers", type=int, default=100_000)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--start", default="2026-09-01")
    ap.add_argument("--skew", type=float, default=1.1, help="Zipf exponent for tower traffic (0 = uniform, higher = more skew)")
    ap.add_argument("--dup-rate", type=float, default=0.01)
    ap.add_argument("--chunk-rows", type=int, default=500_000, help="rows per CSV part file")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    start = pd.Timestamp(args.start)
    raw = os.path.join(args.out, "raw")
    if os.path.isdir(raw):
        shutil.rmtree(raw)  # regenerate cleanly so re-running the generator is deterministic
    os.makedirs(os.path.join(raw, "cdr"))

    towers, customers, plan_changes = make_reference(rng, args.towers, args.customers, start, args.days)
    kpis, degraded = make_kpis(rng, towers, start, args.days)

    towers.to_csv(os.path.join(raw, "towers.csv"), index=False)
    customers.to_csv(os.path.join(raw, "customers.csv"), index=False)
    plan_changes.to_csv(os.path.join(raw, "plan_changes.csv"), index=False)
    PLANS.to_csv(os.path.join(raw, "plans.csv"), index=False)
    kpis.to_csv(os.path.join(raw, "tower_kpis.csv"), index=False)

    # Zipf tower traffic: a handful of towers carry a large share of the records.
    ranks = rng.permutation(args.towers) + 1
    weights = 1.0 / np.power(ranks, args.skew)
    ctx = {
        "n_customers": args.customers,
        "tower_ids": towers["tower_id"].to_numpy(),
        "tower_p": weights / weights.sum(),
        "days": args.days,
        "start_str": start.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    totals = {"rows_raw": 0, "expected_rejected": 0, "expected_duplicates_removed": 0, "expected_loaded": 0}
    by_reason = {r: 0 for r in REJECT_REASONS}
    done, part = 0, 0
    while done < args.rows:
        n = min(args.chunk_rows, args.rows - done)
        chunk, stats = gen_cdr_chunk(rng, n, done + 1, ctx, args.dup_rate)
        chunk.to_csv(os.path.join(raw, "cdr", f"cdr_part-{part:04d}.csv"), index=False)
        for k in totals:
            totals[k] += stats[k]
        for r, v in stats["rejected_by_reason"].items():
            by_reason[r] += v
        done += n
        part += 1
        print(f"  wrote part {part - 1} ({done:,}/{args.rows:,} base rows)")

    top_share = float(np.sort(ctx["tower_p"])[::-1][: max(1, args.towers // 100)].sum())
    manifest = {
        "synthetic": True,
        "seed": args.seed,
        "config": {k: v for k, v in vars(args).items() if k != "out"},
        **totals,
        "rejected_by_reason": by_reason,
        "degraded_towers": degraded,
        "top_1pct_towers_traffic_share": round(top_share, 3),
    }
    manifest["identity_holds"] = (
        totals["rows_raw"] == totals["expected_loaded"] + totals["expected_rejected"] + totals["expected_duplicates_removed"]
    )
    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(json.dumps({k: manifest[k] for k in ["rows_raw", "expected_loaded", "expected_rejected", "expected_duplicates_removed", "top_1pct_towers_traffic_share"]}, indent=2))


if __name__ == "__main__":
    main()
