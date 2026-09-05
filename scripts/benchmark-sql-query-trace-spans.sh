#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkQueryTraceRecorder(Events|OpenTelemetrySpans)$' -benchtime=100x -count=5 -benchmem
