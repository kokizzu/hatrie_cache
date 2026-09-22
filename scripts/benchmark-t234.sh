#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^$' -bench '^BenchmarkT234SQLTransactionEarlyConflict$' -benchmem -benchtime=3s -count=3
