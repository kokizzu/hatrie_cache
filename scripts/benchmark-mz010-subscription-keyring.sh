#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkSQLSubscriptionWireEnvelopeOpen|BenchmarkMZ010SubscriptionWireKeyringOpen)' -benchmem -benchtime=200ms -count=1
