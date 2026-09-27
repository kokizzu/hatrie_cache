#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT024MaterializedTextIndexSelection$' -benchmem -count=5
