#!/bin/sh
set -eu

git diff --check
git add \
    Makefile \
    BENCHMARK.md \
    CH048_STRING_PREDICATE.md \
    ENGINE_IDEAS.md \
    README.md \
    hat/hatSql/columnar_string_predicate.go \
    hat/hatSql/ch048_string_predicate_test.go \
    hat/hatSql/query.go \
    scripts/test-ch048.sh \
    scripts/stage-ch048.sh \
    scripts/commit-ch048.sh \
    scripts/push-ch048.sh
git diff --cached --check
git diff --cached --name-only
