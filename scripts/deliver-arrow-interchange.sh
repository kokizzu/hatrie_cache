#!/bin/sh
set -eu

git diff --check -- Makefile go.mod go.sum SQL.md hat/hatSql/external.go hat/hatSql/external_test.go scripts/add-arrow-dependency.sh scripts/show-arrow-api.sh scripts/tidy-arrow-dependency.sh scripts/test-external-arrow.sh scripts/deliver-arrow-interchange.sh
git add -- Makefile go.mod go.sum SQL.md hat/hatSql/external.go hat/hatSql/external_test.go scripts/add-arrow-dependency.sh scripts/show-arrow-api.sh scripts/tidy-arrow-dependency.sh scripts/test-external-arrow.sh scripts/deliver-arrow-interchange.sh
git commit -m "feat(sql): add Arrow IPC external table interchange"
git push
