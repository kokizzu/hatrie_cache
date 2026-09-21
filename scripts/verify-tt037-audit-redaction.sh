#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatAudit -run '^TestTT037' -count=1
go test ./hat/hatAudit -count=1
