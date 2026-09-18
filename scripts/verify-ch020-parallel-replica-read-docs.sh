#!/usr/bin/env bash
set -eu
test -s CH020_PARALLEL_REPLICA_READ.md
grep -q 'CH-20' INSPIRATION_BACKLOG.md
grep -q 'CH-20' BENCHMARK.md
grep -q 'CH020_PARALLEL_REPLICA_READ.md' README.md
