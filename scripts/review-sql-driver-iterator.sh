#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/client.go hat/hatSql/client_test.go scripts/test-hat-sql-client.sh scripts/format-hat-sql-client.sh scripts/review-sql-driver-iterator.sh scripts/commit-sql-driver-iterator.sh
git diff --name-status -- Makefile hat/hatSql/client.go hat/hatSql/client_test.go scripts/test-hat-sql-client.sh scripts/format-hat-sql-client.sh scripts/review-sql-driver-iterator.sh scripts/commit-sql-driver-iterator.sh
