#!/bin/sh
set -eu
go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkTR049Baseline' -benchmem -count=5
