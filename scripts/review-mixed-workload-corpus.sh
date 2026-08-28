#!/bin/sh
set -eu

git diff --check
git diff --stat -- ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/benchmark-mixed-workload.sh ./scripts/test-mixed-workload-corpus.sh ./scripts/format-mixed-workload-corpus.sh ./scripts/run-mixed-workload-corpus.sh ./cmd/hatrie-sqlbench/main.go ./cmd/hatrie-sqlbench/main_test.go
git status --short
