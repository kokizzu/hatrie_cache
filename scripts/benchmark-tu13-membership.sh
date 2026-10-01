#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench '^BenchmarkTU13' -benchmem -count=5
