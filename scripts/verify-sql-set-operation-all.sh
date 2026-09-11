#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestSQL(SetOperationAll|IntersectAll|ExceptAll)' -count=1
go vet ./hat/hatSql
git diff --check
