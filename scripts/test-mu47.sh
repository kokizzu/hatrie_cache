#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu47 ./hat/hatSql -run '^TestQuerySubscriptionProgressFrame' -count=1
go test ./hat/hatSql
