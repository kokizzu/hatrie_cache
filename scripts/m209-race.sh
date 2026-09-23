#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestM209|TestQuerySubscriptions|TestQueryDifferentialSubscription)' -race -count=1
