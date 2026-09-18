#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ047' -benchmem -count=5
