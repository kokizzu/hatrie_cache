#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md CH051_PREPARED_REGEX_PROGRAMS.md INSPIRATION_BACKLOG.md Makefile README.md \
  hat/hatSql/ch051_regex_program_test.go hat/hatSql/query.go hat/hatSql/regex.go \
  scripts/benchmark-ch051-regex-program.sh scripts/commit-ch051-regex-program.sh \
  scripts/format-ch051-regex-program.sh scripts/push-ch051-regex-program.sh \
  scripts/race-ch051-regex-program.sh scripts/test-ch051-regex-program.sh \
  scripts/vet-ch051-regex-program.sh
git diff --cached --check
git commit -m "sql: prepare literal regex programs"
