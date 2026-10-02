#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-tu21.sh
go test ./hat/hatSchema -count=1 -timeout=180s
go test -race ./hat/hatSchema -run '^TestTU21' -count=1 -timeout=120s
go vet ./hat/hatSchema
git diff --check
