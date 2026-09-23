#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestM213|TestQueryDifferentialSubscription|TestDebezium)' -count=1
