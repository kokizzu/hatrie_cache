#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/template_assertion.go hat/hatSql/template_assertion_test.go scripts/test-query-template-assertions.sh scripts/format-query-template-assertions.sh scripts/review-query-template-assertions.sh scripts/commit-query-template-assertions.sh
git diff --name-status -- Makefile hat/hatSql/template_assertion.go hat/hatSql/template_assertion_test.go scripts/test-query-template-assertions.sh scripts/format-query-template-assertions.sh scripts/review-query-template-assertions.sh scripts/commit-query-template-assertions.sh
