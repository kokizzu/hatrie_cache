#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestStoredProcedureRegistry' -count=1
go test ./hat/hatSql -count=1
go test -race ./hat/hatSql -run 'TestStoredProcedureRegistry' -count=1
go vet ./hat/hatSql
