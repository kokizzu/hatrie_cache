#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestSQLBoundedGroupArray' -count=1
