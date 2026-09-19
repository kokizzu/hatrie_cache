#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC206ResultCacheAdmission' -benchmem -count=5
