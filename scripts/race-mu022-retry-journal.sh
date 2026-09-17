#!/usr/bin/env bash
set -euo pipefail

exec go test -race ./hat/hatSql -run '^TestSQLConnectorTransactionRetryJournal' -count=1
