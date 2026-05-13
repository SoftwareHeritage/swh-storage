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

A running Cassandra cluster with `swh_storage`'s schema applied to a
throw-away keyspace (the script does not clean up after itself).

```bash
# In one shell — bring up Cassandra + schema (example via the test
# fixture; adapt to your cluster).
SWH_CASSANDRA_HOSTS=127.0.0.1 \
SWH_CASSANDRA_KEYSPACE=swh_storage_bench \
SWH_CASSANDRA_PORT=9042
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

- The harness uses synthetic 1024-byte `Content` payloads with
  deterministic hashes (per-iteration seed). It writes the full 5-row
  per-content Cassandra surface (4 indexes + main) and does not touch
  objstorage.
- Per-batch wall time under concurrent insertion is dominated by the
  slowest in-flight request, not the average. p95 is the more telling
  number for production sizing.
