#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench '^BenchmarkT204LeaderForKey$' -benchmem -count=5
