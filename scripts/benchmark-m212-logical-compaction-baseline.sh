#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM212PhysicalCompactionBaseline$' -benchmem -benchtime=1s -count=5
