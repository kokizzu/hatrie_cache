#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateArrangementCheckpoint/(capture|restore|rebuild|json_decode|json_roundtrip)$' -benchmem -count=5 -benchtime=100ms
