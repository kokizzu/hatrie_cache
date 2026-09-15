#!/usr/bin/env bash
set -euo pipefail

git diff --check -- hat/hatDataStructure/compact_posting.go hat/hatDataStructure/compact_posting_test.go hat/hatDataStructure/compact_posting_benchmark_test.go INSPIRATION.md BENCHMARK.md
git diff --stat -- hat/hatDataStructure/compact_posting.go hat/hatDataStructure/compact_posting_test.go hat/hatDataStructure/compact_posting_benchmark_test.go INSPIRATION.md BENCHMARK.md
git diff -- hat/hatDataStructure/compact_posting.go hat/hatDataStructure/compact_posting_test.go hat/hatDataStructure/compact_posting_benchmark_test.go
git status --short
