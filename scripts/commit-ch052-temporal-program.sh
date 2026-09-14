#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md CH052_PREPARED_TEMPORAL_EXPRESSIONS.md hat/hatSql/query.go hat/hatSql/time_zone.go hat/hatSql/ch052_temporal_program_test.go scripts/benchmark-ch052-temporal-program.sh scripts/commit-ch052-temporal-program.sh scripts/format-ch052-temporal-program.sh scripts/push-ch052-temporal-program.sh scripts/race-ch052-temporal-program.sh scripts/test-ch052-temporal-program.sh scripts/vet-ch052-temporal-program.sh
git commit -m "sql: prepare literal time zones"
