#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestSQLApproximate|TestSQLApproximatePercentileInfo)' -count=1
