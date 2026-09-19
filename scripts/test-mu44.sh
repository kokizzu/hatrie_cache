#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu44 ./hat/hatSql -run 'TestSQLDataflowVisibilityCoordinator' -count=1
go test ./hat/hatSql
