#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatCache ./cmd/hatrie-cache -count=1
go test -race ./hat/hatCache -run '^TestMonitoringHandlerJournalCursorResumesAndBinds$' -count=1
go test -race ./cmd/hatrie-cache -run '^TestParseConfigJournalCursorSecretDefaultsOffAndIsRedacted$' -count=1
go vet ./hat/hatCache ./cmd/hatrie-cache
