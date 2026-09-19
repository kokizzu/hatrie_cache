#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkTypedTableArrangementHydration/(hydrate_one_change|rebuild_10000_changes)$' -benchmem -count=5 -benchtime=100ms
