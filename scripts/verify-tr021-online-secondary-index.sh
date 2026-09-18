#!/usr/bin/env bash
set -euo pipefail

rg -q 'func \(source \*MaterializedSource\) BuildSecondaryIndex' hat/hatSchema/materialized.go
rg -q 'TestMaterializedSourceBuildsSecondaryIndexOnline' hat/hatSchema/tr021_online_secondary_index_test.go
rg -q 'BenchmarkTT021OnlineSecondaryIndexBuild' hat/hatSchema/tr021_online_secondary_index_benchmark_test.go
rg -q 'TR-21a' INSPIRATION_BACKLOG.md
rg -q 'T020a' INSPIRATION.md
rg -q 'T020a' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
rg -q 'TR-021 MaterializedSource Online Secondary-Index Build' BENCHMARK.md
rg -q 'TR-21a' TR021_ONLINE_SECONDARY_INDEX.md
