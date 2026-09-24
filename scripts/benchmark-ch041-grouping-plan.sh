#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH041Grouping(SetQuery|BranchClone)$' -benchmem -count=10
