#!/bin/sh
set -eu
export GOCACHE="${GOCACHE:-$PWD/.gocache}"

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU16(TypedTableStringStorage|AdaptiveDictionaryPostChurn)$' -benchmem -count=5
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU16AdaptiveDictionaryDemotion$' -benchtime=100x -benchmem -count=5
