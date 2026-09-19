#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkSinkBackpressureDirectFrontier$' -benchmem -count=5
