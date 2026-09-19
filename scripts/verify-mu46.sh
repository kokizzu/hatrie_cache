#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mu46.sh
go test ./hat/hatSql
go test -tags=mu46 ./hat/hatSql -run 'TestDifferential(Checkpoint|DataflowImport)' -count=1
go test -race -tags=mu46 ./hat/hatSql -run 'TestDifferential(Checkpoint|DataflowImport)' -count=1
go vet -tags=mu46 ./hat/hatSql
