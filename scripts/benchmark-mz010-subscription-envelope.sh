#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSubscriptionWireEnvelope' -benchmem -count=5
