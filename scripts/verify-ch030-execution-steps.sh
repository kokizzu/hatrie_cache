#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -count=1
go test -race ./hat/hatSql -count=1
go vet ./hat/hatSql
