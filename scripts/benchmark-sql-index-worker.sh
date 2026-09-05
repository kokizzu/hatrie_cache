#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONIndexRebuildWorkerStartup$' -benchmem -count=5
