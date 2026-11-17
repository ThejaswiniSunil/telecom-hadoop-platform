# Local (no Docker) workflow. Needs Java 17 or 21 and Python 3.10+.
PY ?= python3
ROWS ?= 1000000
export PYTHONPATH := jobs/spark

install:
	$(PY) -m pip install -r requirements.txt

data:                       ## synthetic data, default 1M rows (ROWS=10000000 for the big run)
	$(PY) generator/generate_telecom.py --out data --rows $(ROWS)

clean-cdr:                  ## raw -> curated + rejects + reconciliation
	$(PY) jobs/spark/clean_cdr.py --root data --shuffle-partitions 16

marts:                      ## curated -> star schema + analytics tables
	$(PY) jobs/spark/build_marts.py --root data --shuffle-partitions 16

dq:                         ## 12 automated data-quality checks (exit 1 on failure)
	$(PY) jobs/spark/dq_checks.py --root data --shuffle-partitions 16

pipeline: clean-cdr marts dq

test:
	$(PY) -m pytest tests -q

bench:
	$(PY) benchmarks/run_benchmarks.py --root data

mr-local:                   ## MapReduce logic without a cluster: cat | map | sort | reduce
	cat data/raw/cdr/*.csv | $(PY) jobs/mapreduce/mapper.py | sort -k1,1 | $(PY) jobs/mapreduce/reducer.py | sort -t "$$(printf '\t')" -k2,2nr | head -10

reset:
	rm -rf data/curated data/marts data/audit data/bench

.PHONY: install data clean-cdr marts dq pipeline test bench mr-local reset
