#!/usr/bin/env bash
set -euo pipefail

rg -q 'type SQLDistributedFrontierCoordinator' hat/hatSql/sql_distributed_frontier.go
rg -q 'BeginSQLDistributedFrontierSnapshot' hat/hatSql/sql_distributed_frontier.go
rg -q 'TestSQLDistributedFrontierCoordinatorCombinesIndependentBarriers' hat/hatSql/sql_distributed_frontier_test.go
rg -q 'BenchmarkM032DistributedFrontierConstruction' hat/hatSql/sql_distributed_frontier_benchmark_test.go
rg -q 'M032 Distributed Frontier Coordinator' M032_DISTRIBUTED_FRONTIER.md
rg -q 'M032e Distributed Frontier Coordinator' BENCHMARK.md
rg -q 'M032e' INSPIRATION.md
rg -q 'M032e' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
