#!/usr/bin/env bash
set -euo pipefail

go test ./...
go test -race ./hat/hatSql ./hat/hatCache
go vet ./hat/hatSql ./hat/hatCache
git diff --check
