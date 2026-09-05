#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLQueryManager' -count=1
