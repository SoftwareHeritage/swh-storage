# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""``content_add`` throughput bench.

Standalone script, not collected by pytest (filename starts with
``bench_``, not ``test_``). Run against a real Cassandra cluster:

    python -m swh.storage.tests.bench.bench_content_add \\
        --hosts 127.0.0.1 --keyspace swh_storage \\
        --batch-sizes 100,500,1000 --iterations 20

For each batch size the script runs ``iterations`` rounds against the
``content_add`` implementation of the checked-out revision, and reports
p50/p95 wall time per batch plus throughput.

To compare two implementations (e.g. before/after a write-path change),
run the script once per checkout with ``--append-csv results.csv`` and a
distinguishing ``--label``; the CSV accumulates rows across runs.

Reads no production data — generates fresh random ``Content`` objects
per round.  Cleans nothing up — point it at a throw-away keyspace.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import hashlib
import os
import sys
import time
from typing import Iterable, List

from swh.model.model import Content

CONTENT_PAYLOAD_LEN = 1024
"""Size in bytes of each synthetic content's data buffer."""


def _generate_contents(n: int, *, seed: int = 0) -> List[Content]:
    """Build ``n`` deterministic synthetic ``Content`` objects.

    Each is a ``CONTENT_PAYLOAD_LEN``-byte buffer of pseudo-random
    bytes; SHA hashes are computed via the canonical
    ``Content.from_data`` constructor so the resulting objects are
    representative of what the loader emits at runtime.
    """
    contents: List[Content] = []
    rng = hashlib.blake2b(seed.to_bytes(8, "little"), digest_size=64).digest()
    for i in range(n):
        chunk = hashlib.blake2b(rng + i.to_bytes(8, "little")).digest()
        # Pad / truncate to the target length so every batch costs the
        # same to checksum on the bench machine.
        data = (chunk * (CONTENT_PAYLOAD_LEN // len(chunk) + 1))[:CONTENT_PAYLOAD_LEN]
        contents.append(Content.from_data(data))
    return contents


@dataclass
class Sample:
    """A single bench measurement."""

    label: str
    batch_size: int
    wall_seconds: float


def _percentile(values: Iterable[float], pct: float) -> float:
    sorted_values = sorted(values)
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def _run_one(storage, contents: List[Content]) -> float:
    """Time a single ``_content_add`` call.  Returns wall time in seconds."""
    t0 = time.monotonic()
    storage._content_add(list(contents), with_data=False)
    return time.monotonic() - t0


def _build_storage(*, hosts: List[str], keyspace: str, port: int):
    # Imported lazily so this module can be imported without
    # cassandra-driver installed for an "--help" run.
    from swh.storage.cassandra import CassandraStorage

    return CassandraStorage(
        hosts=hosts,
        keyspace=keyspace,
        port=port,
        objstorage={"cls": "memory"},
    )


def run_bench(
    *,
    label: str,
    hosts: List[str],
    keyspace: str,
    port: int,
    batch_sizes: List[int],
    iterations: int,
) -> List[Sample]:
    samples: List[Sample] = []
    storage = _build_storage(hosts=hosts, keyspace=keyspace, port=port)
    for batch_size in batch_sizes:
        for i in range(iterations):
            # Fresh seed per round so each iteration hits new keys.
            contents = _generate_contents(
                batch_size, seed=hash((label, batch_size, i)) & 0xFFFFFFFF
            )
            wall = _run_one(storage, contents)
            samples.append(Sample(label, batch_size, wall))
    return samples


def report(samples: List[Sample]) -> None:
    """Aggregate by (label, batch_size) and print a flat table."""
    grouped: dict = {}
    for s in samples:
        grouped.setdefault((s.label, s.batch_size), []).append(s.wall_seconds)

    print(
        f"{'label':<20} {'batch':>6} {'iters':>6} "
        f"{'p50_s':>10} {'p95_s':>10} {'throughput_cnt/s':>20}"
    )
    print("-" * 78)
    for (label, batch_size), walls in sorted(grouped.items()):
        p50 = _percentile(walls, 50)
        p95 = _percentile(walls, 95)
        throughput = batch_size / p50 if p50 > 0 else float("nan")
        print(
            f"{label:<20} {batch_size:>6} {len(walls):>6} "
            f"{p50:>10.4f} {p95:>10.4f} {throughput:>20.1f}"
        )


def append_csv(path: str, samples: List[Sample]) -> None:
    """Append raw samples to ``path``, writing a header if the file is new.

    Accumulating rows across runs (one run per checkout, distinguished
    by ``--label``) is how before/after comparisons are made.
    """
    is_new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(["label", "batch_size", "wall_seconds"])
        for s in samples:
            writer.writerow([s.label, s.batch_size, f"{s.wall_seconds:.6f}"])


def _csv_int_list(s: str) -> List[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def _csv_str_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--hosts",
        type=_csv_str_list,
        default=os.environ.get("SWH_CASSANDRA_HOSTS", "127.0.0.1"),
    )
    parser.add_argument(
        "--keyspace",
        default=os.environ.get("SWH_CASSANDRA_KEYSPACE", "swh_storage_bench"),
    )
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("SWH_CASSANDRA_PORT", "9042"))
    )
    parser.add_argument("--batch-sizes", type=_csv_int_list, default=[100, 500, 1000])
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument(
        "--label",
        default="current",
        help="tag for this run's rows (e.g. a short git revision); "
        "used to tell runs apart in the report and the CSV",
    )
    parser.add_argument(
        "--append-csv",
        metavar="FILE",
        help="append raw samples to FILE (header written if new); "
        "accumulate runs from different checkouts to compare them",
    )
    args = parser.parse_args(argv)

    if isinstance(args.hosts, str):
        args.hosts = _csv_str_list(args.hosts)

    print(
        f"Running content_add bench against {args.hosts}:{args.port}/{args.keyspace} "
        f"— label={args.label} batch_sizes={args.batch_sizes} "
        f"iterations={args.iterations}",
        file=sys.stderr,
    )
    samples = run_bench(
        label=args.label,
        hosts=args.hosts,
        keyspace=args.keyspace,
        port=args.port,
        batch_sizes=args.batch_sizes,
        iterations=args.iterations,
    )
    report(samples)
    if args.append_csv:
        append_csv(args.append_csv, samples)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
