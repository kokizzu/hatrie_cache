#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestSQLBitmap' -count=1
