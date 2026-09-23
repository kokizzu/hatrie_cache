#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestM209|TestQuerySubscriptions|TestQueryDifferentialSubscription)' -count=1
