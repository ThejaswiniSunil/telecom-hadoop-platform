#!/usr/bin/env python3
"""Hadoop Streaming reducer: input arrives sorted by key (the shuffle/sort phase did that).

Emits: tower_id <TAB> records <TAB> dropped <TAB> drop_rate <TAB> revenue
"""
import sys

current, records, dropped, revenue = None, 0, 0, 0.0


def emit():
    if current is not None:
        rate = dropped / records if records else 0.0
        print(f"{current}\t{records}\t{dropped}\t{rate:.4f}\t{revenue:.2f}")


for line in sys.stdin:
    tower, n, d, amount = line.rstrip("\n").split("\t")
    if tower != current:
        emit()
        current, records, dropped, revenue = tower, 0, 0, 0.0
    records += int(n)
    dropped += int(d)
    revenue += float(amount)
emit()
