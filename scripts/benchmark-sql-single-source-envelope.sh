#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLMaterializedSingleSourceEnvelope$' -benchmem -count=5
