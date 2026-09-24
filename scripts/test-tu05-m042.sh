#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLSessionTransactionSettings' -count=1
