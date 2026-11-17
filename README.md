# Telecom Network \& Usage Analytics on Hadoop, Spark and Hive

A small big-data platform you build, break and tune on your own machine. Telecom data is a natural fit for big-data tools: huge volumes, dirty records, and a few very busy towers.

> \*\*100% synthetic.\*\* The generator invents all data. No real operator data, schemas, or system names are used anywhere. The README and CV line should say "simulation inspired by the telecom industry".

## What is verified and what is not

|Part|Status|
|-|-|
|Data generator, Spark jobs (clean, marts, DQ), MapReduce scripts, benchmark harness|**Run and tested** (12 pytest tests pass; full pipeline ran on 1M rows, local mode, 2 cores, about 80 s end to end)|
|Pipeline results vs generator ground truth|**Exact match**: every reject reason count, duplicates removed, and the 4 injected degraded towers found with 0 false alarms|
|`docker/` cluster (HDFS, YARN, Spark standalone, Hive), Hive SQL|**Written but NOT run.** Syntax is valid, but I could not start Docker where this was built. Expect to fix an image tag or a setting. See Troubleshooting|

## Architecture

```
generator (Python, synthetic, defects + skew injected on purpose)
      |
      v
 HDFS  raw/ (CSV landing zone)
      |
      v   Spark: explicit schema, validate, standardise, dedupe
 HDFS  curated/cdr (Parquet, partitioned by call\_date)   curated/cdr\_rejected (reason-coded)
      |                                                    audit/run\_log, rejects\_by\_reason
      v   Spark: Kimball star schema, SCD Type 2 customer plan
 HDFS  marts/ fact\_usage + dim\_date, dim\_tower, dim\_plan, dim\_customer
              agg\_region\_daily, tower\_health (anomaly flags), churn\_features
      |
      v
 Hive external tables + views  ->  BI tool (Power BI / Superset via HiveServer2)

 MapReduce (Hadoop Streaming) computes calls per tower on YARN: the classic model, for comparison.
 Quality gate: 12 automated checks -> audit/dq\_results (pipeline fails if any check fails)
```

## Data model

* **Fact:** `fact\_usage`, one row per call, SMS or data session (grain: one CDR). Partitioned by `call\_date`.
* **Dimensions:** `dim\_date`, `dim\_tower`, `dim\_plan`, `dim\_customer`.
* **SCD Type 2 on `dim\_customer`:** a customer who changes plan gets a new row with `valid\_from`, `valid\_to`, `is\_current`. Each call joins to the plan valid on the call date, so history does not change when a customer upgrades.
* **Unknown member:** an unresolved key becomes `-1` and a data-quality check fails the run, instead of silently dropping the row.

## Part 1: run it locally (no Docker, 10 minutes)

Needs Java 17 or 21 and Python 3.10+.

```bash
make install
make data ROWS=1000000      # 12 s. Writes data/raw/... and data/manifest.json (the ground truth)
make pipeline               # clean-cdr, marts, dq  (about 80 s on 1M rows)
make test                   # 12 tests
make mr-local               # the MapReduce logic as cat | map | sort | reduce
make bench                  # performance experiments, writes benchmarks/results.md
```

Look at `data/manifest.json` after `make data`, then at the `RECONCILIATION:` line from `make clean-cdr`. The numbers match. That is the point of the project: **every row is accounted for**.

## Part 2: run it on a real (mini) cluster

Needs Docker with about 8 GB RAM free. Run these from the repo root.

```bash
cd docker \&\& docker compose up -d --build \&\& cd ..
```

Open the UIs: NameNode `http://localhost:9870` (should show **2 live DataNodes**), YARN `http://localhost:8088`, Spark `http://localhost:8080`.

**1. Generate bigger data and load it into HDFS**

```bash
make data ROWS=10000000
docker compose -f docker/docker-compose.yml exec namenode hdfs dfs -mkdir -p /telecom
docker compose -f docker/docker-compose.yml exec namenode hdfs dfs -put /data/raw /telecom/raw
docker compose -f docker/docker-compose.yml exec namenode hdfs dfs -ls -h /telecom/raw/cdr
docker compose -f docker/docker-compose.yml exec namenode hdfs fsck /telecom/raw/cdr -files -blocks -locations
```

**2. Run the Spark jobs on the Spark cluster, reading and writing HDFS**

```bash
S="docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 4 --executor-memory 1500m"
$S /opt/jobs/spark/clean\_cdr.py   --root hdfs://namenode:8020/telecom --shuffle-partitions 32
$S /opt/jobs/spark/build\_marts.py --root hdfs://namenode:8020/telecom --shuffle-partitions 32
$S /opt/jobs/spark/dq\_checks.py   --root hdfs://namenode:8020/telecom
```

While a job runs, open `http://localhost:4040` and look at stages, tasks, shuffle read/write, and task-time spread.

**3. MapReduce on YARN**

```bash
docker compose -f docker/docker-compose.yml exec nodemanager bash -c \\
 'hadoop jar $(find / -name "hadoop-streaming-\*.jar" 2>/dev/null | head -1) \\
   -files /opt/mr/mapper.py,/opt/mr/reducer.py \\
   -mapper "python3 mapper.py" -reducer "python3 reducer.py" \\
   -input /telecom/raw/cdr -output /telecom/mr/tower\_calls'
docker compose -f docker/docker-compose.yml exec namenode hdfs dfs -cat /telecom/mr/tower\_calls/part-\* | sort -t$'\\t' -k2,2nr | head
```

Compare the output with `agg` results from Spark. Compare the runtime too, and note why Spark is faster (in-memory, no disk write between stages).

**4. Hive**

```bash
docker compose -f docker/docker-compose.yml exec hive beeline -u jdbc:hive2://localhost:10000 -f /opt/sql/01\_ddl.sql
docker compose -f docker/docker-compose.yml exec hive beeline -u jdbc:hive2://localhost:10000 -f /opt/sql/02\_views\_and\_queries.sql
```

## Part 3: the experiments (this is what makes you credible)

Do each one, write down the numbers, and keep a screenshot. One sentence per experiment goes into the README and into your interview answers.

|#|Experiment|How|What you learn|
|-|-|-|-|
|1|**Replication**|Re-put the data with `hdfs dfs -D dfs.replication=1 -put ...`, then `=2`. Compare `fsck` output and disk use|Replication costs space, buys fault tolerance|
|2|**Kill a DataNode**|`docker compose stop datanode2`, then run `hdfs dfs -cat` on a file and re-run a Spark job. Then `start` it and watch `hdfs fsck / \| grep -i under`|Reads survive; the NameNode re-replicates blocks|
|3|**Block size and small files**|`make bench` (small\_files), then repeat on HDFS with thousands of tiny files and look at NameNode memory/metadata and job time|Why HDFS dislikes small files and why we compact|
|4|**File formats**|`make bench` (formats): CSV vs Parquet vs ORC, size and query time|Columnar storage, column pruning|
|5|**Partition pruning**|`make bench` (pruning), plus `EXPLAIN` in Hive. **Run with 10M+ rows**: on 200K rows the difference is noise|Partition on a column you filter by, low cardinality|
|6|**Skew**|`make bench` (skew). Generate with `--skew 1.5`, check the slowest tasks in the Spark UI|One hot tower makes a few tasks slow; AQE, broadcast, salting|
|7|**Shuffle partitions**|Run `clean\_cdr.py` with `--shuffle-partitions` 4, 32, 400|Too few = big tasks and spills; too many = overhead|
|8|**MapReduce vs Spark**|Step 3 above vs the Spark equivalent|Why Spark replaced MapReduce for most work|
|9|**Idempotency**|Run `clean\_cdr.py` twice and check the curated row count does not change (a test covers this)|Safe re-runs|

Record results in `benchmarks/results.md` (the harness writes it) and add one paragraph per surprise.

## Part 4: how to talk about this project

**60-second pitch**

> "I built a small telecom analytics platform on Hadoop and Spark. The data is synthetic: about 10 million call records with defects injected on purpose and a few very busy towers, so the problems are realistic. Spark validates each record against an explicit schema, sends bad rows to a reason-coded reject table, removes duplicates, and writes partitioned Parquet to HDFS. Every run reconciles exactly: raw rows equal loaded plus rejected plus duplicates, and the generator's ground truth confirms it. On top I built a star schema with SCD Type 2 on customer plans, 12 automated data-quality checks, and a tower anomaly table that found all the degraded towers I planted. I also benchmarked formats, partitioning, small files, skew and replica failure, so I can say what each change did and why."

**Questions this project lets you answer with evidence**

* *What happens when a DataNode dies?* Experiment 2.
* *Why Parquet?* Experiment 4 (your own size and time numbers).
* *How do you handle skew?* Experiment 6: spot it in the Spark UI, then broadcast, AQE, or salting.
* *How do you know the pipeline did not lose data?* The reconciliation identity and the manifest.
* *Hadoop vs Spark?* Experiment 8.
* *How is a late or repeated batch handled?* Partition overwrite and `dropDuplicates` on `cdr\_id` make a re-run safe. (Say "idempotent re-runs" and mean "same input, same end state".)

**Be precise about these limits**

* This is a **local multi-container cluster**, not a production deployment.
* The Spark job uses **dynamic partition overwrite** (reruns rewrite the same day partitions) and the run log is append-only. Real dedup across different batches (late data arriving in later files) would need a MERGE into a table format such as Delta, Iceberg or Hudi. Say so if asked, and name it as the next step.
* Spark runs on its **standalone** cluster here. Running Spark on YARN is a worthwhile stretch (the NodeManager image needs Python installed).
* The anomaly rule (latency at 2x a tower's own median for 6+ hours) is a simple statistical rule on planted degradation, not a production detector. The churn table is a feature set, not a model.

**Cloud mapping (shows you know where the industry is going)**

|This project|Cloud equivalent|
|-|-|
|HDFS|S3 / ADLS Gen2 / GCS|
|YARN|Kubernetes, or managed EMR / Dataproc / HDInsight|
|Spark standalone|Databricks, EMR, Synapse|
|Hive external tables + metastore|Glue Catalog, Unity Catalog|
|Parquet partitions|Delta / Iceberg tables (add ACID, MERGE, time travel)|
|`run\_pipeline` via Makefile|Airflow / Databricks Jobs / Azure Data Factory|

**CV wording (adjust to what you actually completed)**

> \*Telecom Network \& Usage Analytics Platform (synthetic data) | Hadoop, HDFS, YARN, Spark, Hive, Parquet, Docker.\* Built a Dockerized HDFS/Spark/Hive stack processing 10M+ simulated call records: explicit-schema validation, reason-coded rejects, exact row reconciliation, star schema with SCD Type 2, and 12 automated data-quality checks. Benchmarked file formats, partition pruning, small-file compaction and join skew, and tested recovery after a DataNode failure.

Fill in your real numbers from the benchmark runs before using this line, and only list what you ran.

## Troubleshooting

* **A container exits immediately:** `docker compose logs <service>`. The most common cause is not enough memory. Lower `yarn.nodemanager.resource.memory-mb` in `docker/hadoop.env` and the Spark worker `--memory` in the compose file.
* **`apache/hadoop:3` tag or its config keys changed:** check the image's page on Docker Hub. The `hadoop.env` key style (`CORE-SITE.XML\_...`) is the one the official image documents.
* **Dockerfile fails at the Python install:** the base image's package manager may differ. Edit the `RUN` line in `docker/Dockerfile.hadoop-python`. If you cannot get Python in, skip the Streaming job and run the built-in example instead: `hadoop jar .../hadoop-mapreduce-examples-\*.jar wordcount <in> <out>`. The mechanics (map, shuffle, reduce) are the same.
* **Hive cannot see HDFS data:** run `MSCK REPAIR TABLE fact\_usage;` again after the Spark job. Make sure the `LOCATION` paths match the folders under `hdfs://namenode:8020/telecom/marts/`.
* **Spark job cannot write to HDFS (permissions):** `dfs.permissions.enabled=false` is set for the lab. If you changed it, create the folders with the right owner.
* **Spark UI at :4040 is empty:** it exists only while a job is running.
* **Local run out of memory:** use `ROWS=500000`, or set `export PYSPARK\_DRIVER\_MEMORY=4g`.

## Repo layout

```
docker/        compose file, Hadoop settings, Dockerfile (Hadoop + Python)
generator/     synthetic data with injected defects and skew, plus ground-truth manifest
jobs/spark/    common.py, transforms.py (pure, tested), clean\_cdr.py, build\_marts.py, dq\_checks.py
jobs/mapreduce/ Hadoop Streaming mapper and reducer (calls and drop rate per tower)
sql/hive/      external table DDL, views, analytical queries
tests/         unit tests + end-to-end test against the generator's ground truth
benchmarks/    experiment harness, results written to results.md / results.csv
```

