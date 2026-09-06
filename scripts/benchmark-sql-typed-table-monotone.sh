#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateApply(Monotone|GeneralInsertOnly)$' -benchmem -count=5
