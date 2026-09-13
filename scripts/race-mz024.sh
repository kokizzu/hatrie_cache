#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestMonitoringHandlerJournalCursorResumesAndBinds$' -count=1
go test -race ./cmd/hatrie-cache -run '^TestParseConfigJournalCursorSecretDefaultsOffAndIsRedacted$' -count=1
