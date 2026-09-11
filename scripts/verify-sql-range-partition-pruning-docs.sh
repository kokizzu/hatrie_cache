#!/usr/bin/env bash
set -euo pipefail

test -s SQL_PARTITION_RANGE_PRUNING.md
rg -n 'SQL_PARTITION_RANGE_PRUNING\.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -n 'CH-003.*Partition-key pruning' ENGINE_IDEAS.md
rg -n 'benchmark-sql-range-partition-pruning' SQL_PARTITION_RANGE_PRUNING.md BENCHMARK.md Makefile
