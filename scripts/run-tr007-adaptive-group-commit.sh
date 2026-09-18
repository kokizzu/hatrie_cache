#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"

case "$mode" in
  format)
    gofmt -w \
      hat/hatJournal/journal.go \
      hat/hatCache/journal.go \
      hat/hatCache/tr007_adaptive_group_commit_test.go \
      hat/hatCache/tr007_adaptive_group_commit_baseline_benchmark_test.go \
      hat/hatCache/tr007_adaptive_group_commit_benchmark_test.go
    ;;
  test)
    go test ./hat/hatCache -run 'TestTR007' -count=1
    ;;
  baseline)
    go test -tags tr007baseline ./hat/hatCache -run '^$' -bench '^BenchmarkTR007GroupCommitFixed$' -benchmem -count=5
    ;;
  benchmark)
    go test ./hat/hatCache -run '^$' -bench '^BenchmarkTR007GroupCommit' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatCache -run 'TestTR007' -count=1
    ;;
  vet)
    go vet ./hat/hatCache
    ;;
  package)
    go test ./hat/hatCache -count=1
    ;;
  status)
    git status --short
    git diff --stat
    git diff --check
    git diff --cached --stat
    git diff --cached --check
    ;;
  stage)
    git add \
      Makefile \
      README.md \
      BENCHMARK.md \
      ADOPTED_QUERY_ENGINE_IDEAS.md \
      INSPIRATION_BACKLOG.md \
      TR007_ADAPTIVE_WAL_GROUP_COMMIT.md \
      hat/hatCache/journal.go \
      hat/hatCache/tr007_adaptive_group_commit_baseline_benchmark_test.go \
      hat/hatCache/tr007_adaptive_group_commit_benchmark_test.go \
      hat/hatCache/tr007_adaptive_group_commit_test.go \
      hat/hatJournal/journal.go \
      scripts/run-tr007-adaptive-group-commit.sh
    ;;
  commit)
    git commit -m 'adopt adaptive WAL group commit [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
