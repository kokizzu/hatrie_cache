#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mu47.sh
go test ./hat/hatSql
go test -tags=mu47 ./hat/hatSql -run '^TestQuerySubscriptionProgressFrame' -count=1
go test -race -tags=mu47 ./hat/hatSql -run '^TestQuerySubscriptionProgressFrame' -count=1
go vet -tags=mu47 ./hat/hatSql
