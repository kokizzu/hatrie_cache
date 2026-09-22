#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^$' -bench '^BenchmarkT233SQLTransactionYield$' -benchmem -benchtime=3s -count=3
