#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	 hat/hatJournal/t018_sync_policy_test.go \
	 hat/hatJournal/journal.go \
	 hat/hatCache/journal.go \
	 journal_sync_api.go \
	 hat/hatCache/t018_wal_sync_policy_test.go \
	 hat/hatCache/t018_wal_sync_policy_benchmark_test.go \
	 cmd/hatrie-cache/main.go \
	 cmd/hatrie-cache/t018_wal_sync_policy_config_test.go
