#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM250' -count=1
go test ./hat/hatSql
go test -race ./hat/hatSql
go vet ./hat/hatSql
git diff --check
