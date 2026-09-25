#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench 'BenchmarkTT005' -benchmem -count=5
