#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQL(Grouping|Rollup|SQL)' -count=1
