#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/contract_harness.go hat/hatSql/contract_harness_test.go scripts/test-sql-contract-harness.sh scripts/format-sql-contract-harness.sh scripts/review-sql-contract-harness.sh scripts/commit-sql-contract-harness.sh
git diff --name-status -- Makefile hat/hatSql/contract_harness.go hat/hatSql/contract_harness_test.go scripts/test-sql-contract-harness.sh scripts/format-sql-contract-harness.sh scripts/review-sql-contract-harness.sh scripts/commit-sql-contract-harness.sh
