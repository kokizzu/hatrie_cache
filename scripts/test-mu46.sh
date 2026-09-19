#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu46 ./hat/hatSql -run 'TestDifferential(Checkpoint|DataflowImport)' -count=1
go test ./hat/hatSql
