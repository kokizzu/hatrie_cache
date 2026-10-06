#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkM14Baseline' -benchmem -count=5
