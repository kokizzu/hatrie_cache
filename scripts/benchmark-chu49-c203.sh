#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU49SkipEnabled(Query|Explain)$' -benchmem -benchtime=200ms -count=9
