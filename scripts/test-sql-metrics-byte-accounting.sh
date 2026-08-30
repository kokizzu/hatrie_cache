#!/bin/sh
set -eu

go test ./hat/hatSql -run '^(TestSQLMetricsByteAccountingRunsOnlyWhenMetricsAreEnabled|TestSQLObservationResultBytesRunOnlyWhenObserved)$' -count=1
