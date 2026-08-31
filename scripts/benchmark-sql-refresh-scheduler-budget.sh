#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkManagedRefreshSchedulerCycleBudget$' -benchmem -count=5
