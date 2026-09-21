#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM211(TypedTableBeginSQLSnapshotAt|SQLFrontierBoundsValidate)$' -benchmem -count=5
