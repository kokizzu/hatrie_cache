#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestTypedTableSQLSnapshotRegistry|TestTypedTableProvidesDirectHistoricalSQLSnapshot)$' -count=1
