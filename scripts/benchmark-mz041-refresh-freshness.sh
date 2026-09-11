#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkManagedRefreshSchedulerStatuses$' -benchmem -benchtime=1s -count=5
