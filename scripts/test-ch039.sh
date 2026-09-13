#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestSQLAutoCountDistinct' -count=1
