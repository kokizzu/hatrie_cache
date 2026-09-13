#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH019MaterializedViewsStorageAdmission$' -benchmem -count=5
