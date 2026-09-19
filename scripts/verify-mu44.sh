#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mu44.sh
go test ./hat/hatSql
go test -tags=mu44 ./hat/hatSql -run 'TestSQLDataflowVisibilityCoordinator' -count=1
go test -race -tags=mu44 ./hat/hatSql -run 'TestSQLDataflowVisibilityCoordinator' -count=1
go vet -tags=mu44 ./hat/hatSql
