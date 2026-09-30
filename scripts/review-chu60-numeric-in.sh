#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== status ====='
git status --short
printf '%s\n' '===== diff check ====='
git diff --check
printf '%s\n' '===== diff stat ====='
git diff --stat
printf '%s\n' '===== implementation diff ====='
git diff -- hat/hatSql/columnar_numeric_predicate.go hat/hatSql/query.go
