#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql ./hat/hatCache
go test -race ./hat/hatSql ./hat/hatCache
go vet ./hat/hatSql ./hat/hatCache
git diff --check
