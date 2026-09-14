#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md CH053_PREPARED_LITERAL_IN.md hat/hatSql/query.go hat/hatSql/in_program.go hat/hatSql/ch053_in_program_test.go scripts/benchmark-ch053-in-program.sh scripts/commit-ch053-in-program.sh scripts/format-ch053-in-program.sh scripts/push-ch053-in-program.sh scripts/race-ch053-in-program.sh scripts/test-ch053-in-program.sh scripts/vet-ch053-in-program.sh
git commit -m "sql: prepare literal IN sets"
