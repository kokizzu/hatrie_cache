#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu45 ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableJoinArrangementAdvisorExact$' -benchmem -count=5
