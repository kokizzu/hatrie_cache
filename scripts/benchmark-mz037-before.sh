#!/bin/sh
set -eu

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ037PerRecordDispatchBaseline$' -benchmem -benchtime=200ms -count=5
