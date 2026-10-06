#!/usr/bin/env bash
set -euo pipefail

mode="${1:-help}"

case "$mode" in
format)
	gofmt -w \
		hat/hatCache/journal.go \
		hat/hatCache/journal_segments.go \
		hat/hatCache/journal_backup_retention.go \
		hat/hatCache/journal_backup_retention_test.go \
		hat/hatCache/journal_backup_retention_benchmark_test.go \
		hat/hatCache/snapshot_manifest.go \
		hat/hatCache/hot_backup.go
	;;
test)
	go test ./hat/hatCache -run 'TestCommandJournalBackupRetentionLease|TestCreateHotBackupBundle' -count=1
	;;
race)
	go test -race ./hat/hatCache -run 'TestCommandJournalBackupRetentionLease|TestCreateHotBackupBundle' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench 'BenchmarkCommandJournalRetentionBoundary' -benchmem -count=5
	;;
baseline)
	go test ./hat/hatBackup
	;;
check)
	git diff --cached --check
	git status --short
	git diff --cached --stat
	;;
stage)
	git add \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		INSPIRATION.md \
		README.md \
		TG21_HOT_BACKUP.md \
		TG22_BACKUP_WAL_RETENTION.md \
		Makefile \
		hat/hatCache/hot_backup.go \
		hat/hatCache/journal.go \
		hat/hatCache/journal_backup_retention.go \
		hat/hatCache/journal_backup_retention_benchmark_test.go \
		hat/hatCache/journal_backup_retention_test.go \
		hat/hatCache/journal_segments.go \
		hat/hatCache/snapshot_manifest.go \
		scripts/benchmark-tg22.sh \
		scripts/format-tg22.sh \
		scripts/test-tg22.sh \
		scripts/tg22.sh
	;;
commit)
	git commit -m 'feat: protect hot backups with WAL retention leases [skip ci]'
	;;
push)
	git push -u origin HEAD
	;;
help)
	printf '%s\n' 'usage: scripts/tg22.sh {format|test|race|vet|benchmark|baseline|check|stage|commit|push}'
	;;
*)
	printf 'unknown TG22 mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
