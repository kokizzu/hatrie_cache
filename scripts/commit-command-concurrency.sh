#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
git diff --check
git add Makefile hat/hatCache/command_concurrency_test.go scripts/show-concurrency-coverage.sh scripts/test-command-concurrency.sh scripts/test-command-concurrency-race.sh scripts/verify-command-concurrency.sh scripts/format-command-concurrency.sh scripts/inspect-command-concurrency.sh scripts/commit-command-concurrency.sh scripts/push-command-concurrency.sh
git diff --cached --check
git commit -m "test: cover concurrent cache commands"
