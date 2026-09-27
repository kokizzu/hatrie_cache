#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestSQLApproximateAggregate|TestCompiledSQLAutomaticNativeGroupedApproximateAggregates|TestCH039GroupedApproxTopK)' -count=1
