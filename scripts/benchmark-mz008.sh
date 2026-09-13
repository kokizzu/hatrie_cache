#!/usr/bin/env bash
set -eu

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ008' -benchmem -count=5
