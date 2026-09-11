#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== CH-001 implementation ====='
sed -n '1,150p' hat/hatSql/prewhere.go
printf '%s\n' '===== CH-001 tests and benchmark ====='
sed -n '1,130p' hat/hatSql/ch001_prewhere_test.go
sed -n '1,130p' hat/hatSql/ch001_prewhere_benchmark_test.go
printf '%s\n' '===== CH-001 documentation ====='
sed -n '1,180p' SQL_PREWHERE.md
printf '%s\n' '===== working tree ====='
git status --short
