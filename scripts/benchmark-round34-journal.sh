#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatJournal -run '^$' -bench 'BenchmarkParallelReplay' -benchmem -count=5
