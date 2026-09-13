#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md INSPIRATION_BACKLOG.md Makefile SQL_NAMED_SETTINGS.md hat/hatSql/named_settings.go hat/hatSql/ch001_named_settings_baseline_benchmark_test.go hat/hatSql/ch001_named_settings_profiles_test.go scripts/commit-ch001.sh scripts/push-ch001.sh scripts/test-ch001.sh
git commit -m "feat(hatSql): add inheritable validated settings profiles"
