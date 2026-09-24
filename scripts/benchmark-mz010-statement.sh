#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ010(ParseSQLSubscriptionStatement|ManualSubscription|AutoSubscription)$' -benchmem -count=5
