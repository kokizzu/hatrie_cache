#!/bin/sh
set -eu

git diff --check -- Makefile SQL.md hat/hatSql/external.go hat/hatSql/external_test.go scripts/test-external-ndjson.sh scripts/deliver-external-ndjson.sh
git add -- Makefile SQL.md hat/hatSql/external.go hat/hatSql/external_test.go scripts/test-external-ndjson.sh scripts/deliver-external-ndjson.sh
git commit -m "feat(sql): add NDJSON external table interchange"
git push
