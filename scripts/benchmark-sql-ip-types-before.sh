#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryIPStringBaseline$' -benchmem -benchtime=200ms -count=5
