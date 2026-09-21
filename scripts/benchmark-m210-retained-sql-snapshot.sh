#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM210TypedTable(BeginSQLSnapshotAt|SQLSnapshotRegistry)$' -benchmem -count=5
