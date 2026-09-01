#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTemporalTable(ChronologicalUpsert|AsOfLatest)$' -benchmem -count=5
