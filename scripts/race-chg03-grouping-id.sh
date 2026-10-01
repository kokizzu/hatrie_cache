#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestSQL(Grouping|Rollup|SQL)' -count=1
