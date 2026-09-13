#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkC202(Global|Partitioned)AsyncBatcher(NoWork)?$' -benchmem -count=5 -benchtime=1s
