#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestSQLApproximate|TestSQLApproximatePercentileInfo)' -count=1
