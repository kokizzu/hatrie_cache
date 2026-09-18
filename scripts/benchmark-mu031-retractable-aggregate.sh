#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench 'BenchmarkMU031AggregateStateCreation/capability_(retractable|serializable)$' \
  -benchmem \
  -benchtime=1s \
  -count=5 \
  -cpu=1
