#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md CH030_PREPARED_JSON_PATHS.md INSPIRATION_BACKLOG.md Makefile README.md \
  hat/hatSql/ch030_json_path_program_test.go hat/hatSql/json_path.go hat/hatSql/query.go \
  scripts/benchmark-ch030-json-path-program.sh scripts/commit-ch030-json-path-program.sh \
  scripts/format-ch030-json-path-program.sh scripts/race-ch030-json-path-program.sh \
  scripts/push-ch030-json-path-program.sh scripts/test-ch030-json-path-program.sh \
  scripts/vet-ch030-json-path-program.sh
git diff --cached --check
git commit -m "sql: prepare literal JSON paths"
