#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCompiledSQLAutomaticNativeGroupedApproximateAggregates' -count=1
