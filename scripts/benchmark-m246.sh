#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkM246FrontierRetention' -benchmem -count=10 ./hat/hatPipeline
