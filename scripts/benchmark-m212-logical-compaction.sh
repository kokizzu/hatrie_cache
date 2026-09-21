#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM212LogicalCompaction(Transition)?$' -benchmem -benchtime=1s -count=5
