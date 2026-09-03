#!/bin/sh
set -eu
go test ./hat/hatCache -run '^$' -bench '^BenchmarkHatTrieSQLKeysetPaginationDeepPage$' -benchmem -count=5
