#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu44baseline ./hat/hatSql -run '^$' -bench '^BenchmarkSQLDataflowVisibilityBaseline' -benchmem -count=5
