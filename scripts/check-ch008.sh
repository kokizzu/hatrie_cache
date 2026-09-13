#!/usr/bin/env bash
set -euo pipefail
git diff --check
go test ./hat/hatSql -count=1
go test -race ./hat/hatSql -count=1
go vet ./hat/hatSql
