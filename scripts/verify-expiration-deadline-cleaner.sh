#!/usr/bin/env bash
set -euo pipefail

rg -n 'StartExpirationCleaner|deadline-aware|earlier deadline' README.md EXPIRATION_CLEANER.md
rg -n 'TT-047.*Partially adopted|TT-047 Deadline-Aware' ENGINE_IDEAS.md BENCHMARK.md
rg -n 'TestExpirationCleaner|BenchmarkExpirationCleanerDeadlineScheduling' hat/hatCache/expiration_deadline_cleaner_test.go
