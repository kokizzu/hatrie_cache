#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
  format)
    gofmt -w hat/hatSql/c212_hash_join.go hat/hatSql/c212_hash_join_bucket_test.go hat/hatSql/query.go
    ;;
  test)
    go test ./hat/hatSql -run '^TestC212JoinHashIndexKeepsSingletonInlineBeforeGrowing$' -count=1
    ;;
  test-all)
    go test ./hat/hatSql -run 'C212' -count=1
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  all)
    go_cache=$(mktemp -d /tmp/hatrie-ch065-gocache.XXXXXX)
    trap 'rm -rf "$go_cache"' EXIT
    GOCACHE="$go_cache" go test ./... -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run 'C212' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkC212(JoinIndex|CanonicalMap)' -benchmem -count=5
    ;;
  verify-docs)
    rg -F -q 'C085a Compact singleton hash-join buckets' INSPIRATION.md
    rg -F -q 'CH-065 Compact SQL Hash-Join Buckets' BENCHMARK.md
    rg -F -q 'CH065_COMPACT_HASH_JOIN_BUCKETS.md' BENCHMARK.md
    ;;
  status)
    git status --short
    git diff --stat
    git diff --check
    ;;
  stage)
    git add BENCHMARK.md CH065_COMPACT_HASH_JOIN_BUCKETS.md INSPIRATION.md Makefile hat/hatSql/c212_hash_join.go hat/hatSql/c212_hash_join_bucket_test.go hat/hatSql/query.go scripts/ch065-hash-join-buckets.sh
    git diff --cached --check
    git diff --cached --stat
    ;;
  commit)
    git commit -m 'perf(sql): compact hash join buckets [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {format|test|test-all|package|all|race|vet|benchmark|verify-docs|status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
