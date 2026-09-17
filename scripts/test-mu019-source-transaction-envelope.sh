#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLSourceTransactionEnvelope' -count=1
