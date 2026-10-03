#!/usr/bin/env bash
set -euo pipefail

paths=(
	BENCHMARK.md
	Makefile
	README.md
	TG18_WAL_SYNC_POLICY.md
	journal_sync_api.go
	cmd/hatrie-cache/main.go
	cmd/hatrie-cache/t018_wal_sync_policy_config_test.go
	hat/hatCache/journal.go
	hat/hatCache/t018_wal_sync_policy_benchmark_test.go
	hat/hatCache/t018_wal_sync_policy_test.go
	hat/hatJournal/journal.go
	hat/hatJournal/t018_sync_policy_test.go
	scripts/benchmark-t018-durable.sh
	scripts/benchmark-t018-fast.sh
	scripts/benchmark-t018-wal-sync-policy-noop.sh
	scripts/benchmark-t018-wal-sync-policy.sh
	scripts/commit-t018-wal-sync-policy.sh
	scripts/format-t018-wal-sync-policy.sh
	scripts/push-t018-wal-sync-policy.sh
	scripts/race-t018-wal-sync-policy.sh
	scripts/test-t018-wal-sync-policy.sh
	scripts/vet-t018-wal-sync-policy.sh
)

git add "${paths[@]}"
git diff --cached --check
git diff --cached --stat
git commit -m 'feat(journal): add configurable WAL sync policy [skip ci]'
