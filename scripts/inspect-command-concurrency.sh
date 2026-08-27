#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
git diff --check
git diff -- Makefile hat/hatCache/command_concurrency_test.go scripts/show-concurrency-coverage.sh scripts/test-command-concurrency.sh scripts/test-command-concurrency-race.sh scripts/verify-command-concurrency.sh scripts/format-command-concurrency.sh scripts/inspect-command-concurrency.sh
git status --short
