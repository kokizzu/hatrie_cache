#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle -run '^$' -bench '^BenchmarkCH024' -benchmem -count=3
