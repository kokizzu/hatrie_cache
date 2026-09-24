#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestMZ010(ParseSQLSubscriptionStatement|TailStatement)' -count=1
