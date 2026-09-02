#!/bin/sh
set -eu

count=${COUNT:-5}
benchtime=${BENCHTIME:-100ms}
go test ./hat/hatCache \
	-run '^$' \
	-bench 'BenchmarkMonitoring(CommandHTTP|AsyncCommandHTTPAdmission)$' \
	-benchmem \
	-count "$count" \
	-benchtime "$benchtime"
