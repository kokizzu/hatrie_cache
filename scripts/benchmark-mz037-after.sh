#!/bin/sh
set -eu

go test ./hat/hatPipeline \
	-run '^$' \
	-bench '^BenchmarkMZ037(PerRecordDispatchBaseline|WorkerLocalExchange)$' \
	-benchmem \
	-benchtime=200ms \
	-count=5
