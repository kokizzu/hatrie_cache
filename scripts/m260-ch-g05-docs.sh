#!/usr/bin/env bash
set -euo pipefail

test -s CH005_RUNTIME_JOIN_PARTITION_FILTER.md
rg -n 'CH005_RUNTIME_JOIN_PARTITION_FILTER|RuntimeJoinPartitionFilter' README.md CH005_RUNTIME_JOIN_PARTITION_FILTER.md
rg -n '^## CH-G05: Runtime Join Partition Bounds|CH005_RUNTIME_JOIN_PARTITION_FILTER.md' BENCHMARK.md
rg -n 'CH-G05.*Implemented' IDEA_GAP_CATALOG.md
