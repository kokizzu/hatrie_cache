#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMetrics ./hat/hatCache
go test -timeout=120s ./...
go test -race ./hat/hatMetrics ./hat/hatCache
go vet ./hat/hatMetrics ./hat/hatCache
git diff --check
