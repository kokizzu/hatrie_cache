#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLMetricsByteAccountingRunsOnlyWhenMetricsAreEnabled$' -count=1
