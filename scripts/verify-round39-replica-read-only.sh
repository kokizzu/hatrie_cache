#!/usr/bin/env bash
set -euo pipefail

for path in \
    TU06_REPLICA_READ_ONLY.md \
    BENCHMARK.md \
    README.md \
    PRODUCT_IDEA_GAPS.md \
    hat/hatCache/replica_read_only.go \
    hat/hatCache/tu06_replica_read_only_test.go \
    scripts/test-round39-replica-read-only.sh; do
    test -f "$path"
done

rg -n '^# Replica-wide read-only enforcement$' TU06_REPLICA_READ_ONLY.md
rg -n 'TU06_REPLICA_READ_ONLY.md' README.md PRODUCT_IDEA_GAPS.md
rg -n 'T-U06 Replica-wide read-only enforcement' BENCHMARK.md
rg -n 'ErrReplicaReadOnly|SetReplicaReadOnly|ReplicaReadOnly' hat/hatCache/replica_read_only.go
rg -n 'BenchmarkTU06Baseline|BenchmarkTU06ReplicaReadOnly' hat/hatCache/*tu06*test.go
if rg -n 'round39_compile_compat|inspect-round39-' Makefile hat/hatSql 2>/dev/null; then
    printf '%s\n' 'temporary round39 inspection or compile-shim files remain' >&2
    exit 1
fi
printf '%s\n' 'round39 replica read-only documentation verified'
