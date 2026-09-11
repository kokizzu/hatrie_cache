#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestSQLMaxGroupKeys.*|TestSQLColumnarVectorGroupAggregateHonorsGroupKeyLimit)$' -count=1
