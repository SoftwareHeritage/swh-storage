# `swh.storage.tests.bench` — performance harnesses

Standalone bench scripts for the storage hot paths. Not collected by pytest
(filenames begin with `bench_`, not `test_`); run them directly.

## `bench_content_add.py`

Times `CassandraStorage._content_add` for a sweep of batch sizes
(default 100 / 500 / 1000) and reports p50/p95 wall time per batch plus
throughput.

The script measures whatever implementation is checked out. To compare
two implementations (e.g. master before and after a write-path change
such as !1222), run it once per checkout with a distinguishing
`--label` and a shared `--append-csv` file; the CSV accumulates raw
samples across runs.

### Prerequisites

A running Cassandra cluster with `swh-storage`'s schema applied to a
**throw-away keyspace** — the script writes real rows and does **not**
clean up after itself, so never point it at a production keyspace.

Connection parameters come from the `--hosts` / `--keyspace` / `--port`
flags, or from the matching environment variables (exported below,
then picked up as defaults):

```bash
export SWH_CASSANDRA_HOSTS=127.0.0.1
export SWH_CASSANDRA_KEYSPACE=swh_storage_bench
export SWH_CASSANDRA_PORT=9042
```

### Run

```bash
python -m swh.storage.tests.bench.bench_content_add
```

Or with explicit arguments:

```bash
python -m swh.storage.tests.bench.bench_content_add \
    --hosts 127.0.0.1 \
    --keyspace swh_storage_bench \
    --batch-sizes 100,500,1000 \
    --iterations 20 \
    --label "$(git rev-parse --short HEAD)" \
    --append-csv results.csv
```

### Output

```
label                 batch  iters      p50_s      p95_s     throughput_cnt/s
------------------------------------------------------------------------------
7e4a4cb3                100     20      ...        ...                  ...
7e4a4cb3                500     20      ...        ...                  ...
7e4a4cb3               1000     20      ...        ...                  ...
```

### Comparing two revisions

```bash
git checkout <before> && python -m swh.storage.tests.bench.bench_content_add \
    --label before --append-csv results.csv
git checkout <after>  && python -m swh.storage.tests.bench.bench_content_add \
    --label after  --append-csv results.csv
# then compare p50 per (label, batch_size) in results.csv
```

Comparisons across heterogeneous hardware are not meaningful; quote the
cluster details when sharing results.

### Notes

- The harness uses synthetic 1024-byte `Content` payloads with a
  reproducible per-round seed (stable across processes, distinct per
  `label`/batch/round so each run writes fresh keys). It writes the
  full 5-row per-content Cassandra surface (4 indexes + main) and does
  not touch objstorage.
- The first round of each batch size is a discarded warmup, so
  connection-pool and prepared-statement priming does not skew the
  reported percentiles.
- Wall time — not CPU time — is the metric here: the `content_add`
  write path is I/O-bound on Cassandra round-trips, so client-observed
  wall time is what governs ingestion throughput; CPU time would
  understate the cost of the network waits we care about.
- Per-batch wall time under concurrent insertion is dominated by the
  slowest in-flight request, not the average. p95 is the more telling
  number for production sizing.
