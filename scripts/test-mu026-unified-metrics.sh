#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatSql -run 'TestSQLDataflowMetricsCatalog' -count=1
