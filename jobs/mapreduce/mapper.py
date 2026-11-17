#!/usr/bin/env python3
"""Hadoop Streaming mapper: raw CDR lines -> (tower_id, "1 <dropped> <charge>").

Skips the header and malformed lines instead of crashing, because raw data is dirty by design.
Input columns: cdr_id,caller_id,callee_number,tower_id,call_type,start_ts,duration_sec,data_mb,status,charge_aed
"""
import sys

for line in sys.stdin:
    f = line.rstrip("\n").split(",")
    if len(f) != 10 or f[0] == "cdr_id":
        continue
    tower, status, charge = f[3].strip(), f[8].strip().upper(), f[9].strip()
    if not tower:
        continue
    try:
        amount = float(charge) if charge else 0.0
    except ValueError:
        continue
    print(f"{tower}\t1\t{1 if status == 'DROPPED' else 0}\t{amount}")
