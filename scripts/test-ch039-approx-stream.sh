#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLApproximateAggregatesUseStreamingState$' -count=1
go test ./hat/hatSql -count=1
