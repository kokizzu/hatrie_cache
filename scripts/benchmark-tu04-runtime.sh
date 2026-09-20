#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatRuntime -run '^$' -bench 'BenchmarkTU04' -benchmem -count=5
