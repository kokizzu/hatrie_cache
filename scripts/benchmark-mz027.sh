#!/usr/bin/env bash
set -eu

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ027' -benchmem -count=5
