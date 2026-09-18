#!/usr/bin/env bash
set -euo pipefail

files=(
  Makefile
  README.md
  BENCHMARK.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  PRODUCT_IDEA_GAPS.md
  TU36_SNAPSHOT_ROTATION.md
  api.go
  hat/hatCache/backup_bundle.go
  hat/hatCache/backup_repository.go
  hat/hatCache/tu36_backup_rotation.go
  hat/hatCache/tu36_backup_rotation_test.go
  hat/hatCache/tu36_backup_rotation_baseline_benchmark_test.go
  hat/hatCache/tu36_backup_rotation_benchmark_test.go
  scripts/run-tu36-backup-rotation.sh
)

case "${1:-test}" in
  format)
    gofmt -w hat/hatCache/tu36_backup_rotation.go \
      hat/hatCache/tu36_backup_rotation_test.go \
      hat/hatCache/tu36_backup_rotation_baseline_benchmark_test.go \
      hat/hatCache/tu36_backup_rotation_benchmark_test.go
    ;;
  test)
    go test -tags tu36_backup_rotation ./hat/hatCache -run '^TestTU36BackupRotation' -count=1
    ;;
  baseline)
    go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU36BackupRotationBaseline$' -benchmem -count=5
    ;;
  benchmark)
    go test -tags tu36_backup_rotation ./hat/hatCache -run '^$' -bench '^BenchmarkTU36BackupRotation' -benchmem -count=5
    ;;
  race)
    go test -race -tags tu36_backup_rotation ./hat/hatCache -run '^TestTU36BackupRotation' -count=1
    ;;
  vet)
    go vet ./hat/hatCache
    ;;
  package)
    go test -tags tu36_backup_rotation ./hat/hatCache
    ;;
  root)
    go test .
    ;;
  review)
    git diff --check -- "${files[@]}"
    ;;
  stage)
    git add -- "${files[@]}"
    ;;
  commit)
    git commit --only -m 'feat: add snapshot rotation policy [skip ci]' -- "${files[@]}"
    ;;
  push)
    git push origin HEAD:master
    ;;
  status)
    git status --short
    ;;
  *)
    printf 'unknown mode: %s\n' "$1" >&2
    exit 2
    ;;
esac
