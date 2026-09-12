#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkQuerySubscriptionDifferentialPayload$' -benchmem -benchtime=200ms -count=5 ./hat/hatSql
