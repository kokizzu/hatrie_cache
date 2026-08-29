#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExternalTablesExportTransfer$' -benchmem -count=1
