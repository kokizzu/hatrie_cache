#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLGroupingSetsGroupingIdentifiers$' -count=1
go test ./hat/hatSql -count=1
