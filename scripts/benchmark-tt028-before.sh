#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT028LegacyManualMerge$' -benchmem -count=5
