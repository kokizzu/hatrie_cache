#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatJournal/journal.go \
	hat/hatJournal/journal_retention_test.go \
	hat/hatCache/journal.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_retention_test.go \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/main_test.go \
	journal_retention_api.go \
	journal_retention_api_test.go
