#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestSQLSession(TransactionSettings|ReadOnly)' -count=1
