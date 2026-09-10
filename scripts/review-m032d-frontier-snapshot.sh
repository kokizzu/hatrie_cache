#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestBeginSQLFrontierSnapshot' -count=1
go vet ./hat/hatSql
rg -n 'BeginSQLFrontierSnapshot|SQLFrontierSnapshotProvider|M032d|benchmark-m032d-frontier-snapshot' \
	README.md SQL_FRONTIER_SNAPSHOTS.md SQL_SNAPSHOT_PROVIDER.md SQL_SOURCE_FRONTIERS.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
git diff --check
git status --short
