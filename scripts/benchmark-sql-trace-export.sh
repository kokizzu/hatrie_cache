#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkQueryTraceRecorder' -benchmem -count=5
