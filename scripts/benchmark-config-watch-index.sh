#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench 'BenchmarkConfigWatchReadResume' -benchmem -count=5
