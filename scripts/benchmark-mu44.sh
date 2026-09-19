#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu44 ./hat/hatSql -run '^$' -bench '^BenchmarkSQLDataflowVisibility(Publish|Acquire|Check|Snapshot)' -benchmem -count=5
