#!/bin/sh
set -eu

git add ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/benchmark-mixed-workload.sh ./scripts/test-mixed-workload-corpus.sh ./scripts/format-mixed-workload-corpus.sh ./scripts/run-mixed-workload-corpus.sh ./scripts/review-mixed-workload-corpus.sh ./scripts/commit-mixed-workload-corpus.sh ./cmd/hatrie-sqlbench/main.go ./cmd/hatrie-sqlbench/main_test.go
git commit -m 'feat: add mixed cache workload benchmark'
git push
