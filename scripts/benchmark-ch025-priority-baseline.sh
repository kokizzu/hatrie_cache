#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCH025BaselineExplicitPrioritySchedule$' -benchmem -count=5
