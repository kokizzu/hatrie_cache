#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkTU05SessionExecute(Baseline|WithTimeout)$' -benchmem -count=5
