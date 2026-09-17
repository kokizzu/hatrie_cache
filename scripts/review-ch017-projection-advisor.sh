#!/bin/sh
set -eu
git diff --check
git diff --cached --check
git diff --cached --name-status
rg -n '^## Workload-Driven Projection Advice$' README.md
rg -n '^## CH-17 Workload-Driven Projection Advisor$' BENCHMARK.md
rg -n '^\| CH-17 .*\[x\]' INSPIRATION_BACKLOG.md
git diff --stat -- hat/hatSql/projection_advisor.go hat/hatSql/ch017_projection_advisor_test.go hat/hatSql/ch017_projection_advisor_benchmark_test.go CH017_PROJECTION_ADVISOR.md BENCHMARK.md INSPIRATION_BACKLOG.md README.md Makefile scripts
