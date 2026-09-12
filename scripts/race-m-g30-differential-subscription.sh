#!/usr/bin/env bash
set -euo pipefail

go test -race -count=1 ./hat/hatSql -run '^Test(QueryDifferentialSubscription|QuerySubscription)'
