#!/usr/bin/env bash
set -euo pipefail

go test -tags mu37baseline ./hat/hatStorage -run '^$' -bench '^BenchmarkMU37CompactionCountersBaseline$' -benchmem -count=5
