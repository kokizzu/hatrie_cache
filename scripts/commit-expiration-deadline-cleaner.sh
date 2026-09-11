#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md ENGINE_IDEAS.md EXPIRATION_CLEANER.md Makefile README.md \
  hat/hatCache/expiration_cleaner.go \
  hat/hatCache/expiration_deadline_cleaner_test.go \
  hat/hatCache/main.go \
  scripts/benchmark-expiration-deadline-cleaner.sh \
  scripts/commit-expiration-deadline-cleaner.sh \
  scripts/format-expiration-deadline-cleaner.sh \
  scripts/push-expiration-deadline-cleaner.sh \
  scripts/review-expiration-deadline-cleaner.sh \
  scripts/test-expiration-deadline-cleaner-broad.sh \
  scripts/test-expiration-deadline-cleaner.sh \
  scripts/test-race-expiration-deadline-cleaner.sh \
  scripts/verify-expiration-deadline-cleaner.sh \
  scripts/vet-expiration-deadline-cleaner.sh
git commit -m "feat: make expiration cleaner deadline aware"
