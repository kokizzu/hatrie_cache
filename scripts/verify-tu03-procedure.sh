#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-tu03-procedure.sh
go test ./hat/hatProcedure -count=1
go test -race ./hat/hatProcedure -run 'TestRegistry' -count=1
go vet ./hat/hatProcedure
git diff --check
