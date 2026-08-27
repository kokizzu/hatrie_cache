#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
git diff --check
git add Makefile cmd/hatrie-cli/main.go cmd/hatrie-cli/sql_repl.go cmd/hatrie-cli/sql_repl_test.go scripts/test-cli-sql-repl.sh scripts/format-cli-sql-repl.sh scripts/commit-cli-sql-repl.sh scripts/push-cli-sql-repl.sh
git diff --cached --check
git commit -m "feat: add interactive SQL CLI"
