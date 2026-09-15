#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check -- \
  hat/hatDataStructure/compact_posting.go \
  hat/hatDataStructure/compact_posting_test.go \
  hat/hatDataStructure/compact_posting_benchmark_test.go \
  hat/hatDataStructure/functional_index.go \
  hat/hatDataStructure/hash_index.go \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/conditional_index.go \
  TR053_COMPACT_POSTING_LIST.md \
  BENCHMARK.md README.md INSPIRATION_BACKLOG.md
git diff --stat -- \
  hat/hatDataStructure/compact_posting.go \
  hat/hatDataStructure/compact_posting_test.go \
  hat/hatDataStructure/compact_posting_benchmark_test.go \
  hat/hatDataStructure/functional_index.go \
  hat/hatDataStructure/hash_index.go \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/conditional_index.go \
  TR053_COMPACT_POSTING_LIST.md \
  BENCHMARK.md README.md INSPIRATION_BACKLOG.md
printf '%s\n' '--- staged paths ---'
git diff --cached --check
git diff --cached --name-only
git diff --cached --stat
