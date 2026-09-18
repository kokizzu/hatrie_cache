#!/bin/sh
set -eu
go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkTR049(Baseline|Ownership)' -benchmem -count=5
