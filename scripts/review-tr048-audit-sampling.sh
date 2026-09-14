#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test -count=1 ./hat/hatAudit ./hat/hatCache ./cmd/hatrie-cache
go vet ./hat/hatAudit ./hat/hatCache ./cmd/hatrie-cache
