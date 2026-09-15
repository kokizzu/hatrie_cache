#!/usr/bin/env bash
set -euo pipefail

git diff --check -- hat/hatDataStructure/compact_posting.go hat/hatDataStructure/compact_posting_test.go hat/hatDataStructure/compact_posting_benchmark_test.go INSPIRATION.md BENCHMARK.md
git diff --stat -- hat/hatDataStructure/compact_posting.go hat/hatDataStructure/compact_posting_test.go hat/hatDataStructure/compact_posting_benchmark_test.go INSPIRATION.md BENCHMARK.md
git diff -- hat/hatDataStructure/compact_posting.go hat/hatDataStructure/compact_posting_test.go hat/hatDataStructure/compact_posting_benchmark_test.go
git diff -- Makefile
git diff --cached --stat -- Makefile scripts/inspect-index-c216.sh
git diff --cached -- Makefile scripts/inspect-index-c216.sh
git status --short
ps -ef | rg 'BenchmarkC216|benchmark-index-c216|go test ./hat/hatDataStructure'
