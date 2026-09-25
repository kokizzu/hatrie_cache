#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT024TextIndex' -benchmem -benchtime=200ms -count=5
