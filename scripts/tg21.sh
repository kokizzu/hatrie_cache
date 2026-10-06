#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  format)
    gofmt -w hat/hatCache/hot_backup.go hat/hatCache/hot_backup_test.go hat/hatCache/hot_backup_benchmark_test.go
    ;;
  baseline)
    go test ./hat/hatBackup -count=1
    ;;
  test)
    go test ./hat/hatBackup -count=1
    go test ./hat/hatCache -run '^TestCreateHotBackupBundleCapturesOnlineSnapshotManifest$' -count=1
    ;;
  race)
    go test -race ./hat/hatBackup -count=1
    go test -race ./hat/hatCache -run '^TestCreateHotBackupBundleCapturesOnlineSnapshotManifest$' -count=1
    ;;
  vet)
    go vet ./hat/hatBackup
    go vet ./hat/hatCache
    ;;
  benchmark)
    go test ./hat/hatCache -run '^$' -bench '^Benchmark(CreateHotBackupBundle|CreateBlockingBackupBundle)$' -benchmem -count=5
    ;;
  status)
    git diff --check
    git diff --cached --check
    git status --short
    ;;
  diff)
    git diff --stat
    git diff --cached --stat
    git diff --cached -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION.md README.md TG21_HOT_BACKUP.md hat/hatCache/hot_backup.go hat/hatCache/hot_backup_test.go hat/hatCache/hot_backup_benchmark_test.go scripts/tg21.sh
    ;;
  stage)
    git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION.md Makefile README.md TG21_HOT_BACKUP.md hat/hatCache/hot_backup.go hat/hatCache/hot_backup_test.go hat/hatCache/hot_backup_benchmark_test.go scripts/tg21.sh
    ;;
  commit)
    git commit -m "feat: add online hot backup bundle [skip ci]"
    ;;
  push)
    git push --set-upstream origin "$(git branch --show-current)"
    ;;
  *)
    printf '%s\n' 'usage: format|baseline|test|race|vet|benchmark|status|diff|stage|commit|push' >&2
    exit 2
    ;;
esac
