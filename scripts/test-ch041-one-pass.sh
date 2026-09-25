#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestCH041GroupingSets|TestSQLGroupingSetsUsesOnePassPlan)$' -count=1
