#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCompiledSQLAutomaticNativeGroupedApproximateAggregates' -count=1
