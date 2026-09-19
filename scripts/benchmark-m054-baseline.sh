#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLMultiSourceSnapshotControlResolve$' -benchmem -count=5
