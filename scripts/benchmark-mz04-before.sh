#!/usr/bin/env bash
set -eu

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ04FrontierRetentionSnapshot$' -benchmem -benchtime=100000x -count=5
