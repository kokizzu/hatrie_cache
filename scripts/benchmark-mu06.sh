#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialWindow' -benchmem -count=5 -benchtime=100ms
