#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'Differential|Incremental'
go test -race ./hat/hatSql -run 'Differential|Incremental'
go vet ./hat/hatSql
test -f MZ034_GENERIC_NEGATIVE_DIFF.md
git diff --check
