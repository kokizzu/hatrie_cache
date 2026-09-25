#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLProjectionAdvisorSnapshot' -benchmem -benchtime=200ms -count=1
