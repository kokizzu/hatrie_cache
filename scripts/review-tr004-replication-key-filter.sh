#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/format-tr004-replication-key-filter.sh
go test ./hat/hatCache ./cmd/hatrie-cache -run '^TestTR004' -count=1
go test -race ./hat/hatCache ./cmd/hatrie-cache -run '^TestTR004' -count=1
go vet ./hat/hatCache ./cmd/hatrie-cache
git diff --check
git status --short
