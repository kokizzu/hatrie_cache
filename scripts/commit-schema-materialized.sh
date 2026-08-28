#!/usr/bin/env sh
set -eu

git add Makefile SQL.md hat/hatSchema/materialized.go hat/hatSchema/materialized_test.go scripts/commit-schema-materialized.sh scripts/format-schema-materialized.sh scripts/test-schema-materialized.sh
git commit -m 'feat: add materialized derived schema sources'
git push
