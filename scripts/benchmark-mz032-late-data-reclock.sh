#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ032' -benchmem -count=5
