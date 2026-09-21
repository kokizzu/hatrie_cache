#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkMZ050' -benchmem -benchtime=500ms -count=5
