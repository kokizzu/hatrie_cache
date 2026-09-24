#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkMU031Aggregate(DirectAdd|TransactionAdd|TransactionCreate)$' -benchmem -count=5
