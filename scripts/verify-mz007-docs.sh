#!/usr/bin/env bash
set -euo pipefail

rg -n 'SQL_SOURCE_FRONTIERS\.md|RequireSourceFrontier|RequiredSourceFrontier|SQLSourceFrontierResolver|MZ-007|MZ-007 source frontier requirement' README.md SQL_SOURCE_FRONTIERS.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatSql/query.go hat/hatSql/contracts.go sql_source_frontier_api.go
sed -n '60,112p' README.md
sed -n '1,140p' SQL_SOURCE_FRONTIERS.md
sed -n '20450,20485p' BENCHMARK.md
sed -n '1,260p' hat/hatSql/sql_source_frontier_barrier.go
sed -n '1,240p' hat/hatSql/sql_frontier_snapshot_provider.go
