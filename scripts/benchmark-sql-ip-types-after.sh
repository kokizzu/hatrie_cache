#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryIP' -benchmem -benchtime=200ms -count=5
