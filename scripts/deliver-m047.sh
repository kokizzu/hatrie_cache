#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add Makefile ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md PRODUCT_IDEA_GAPS.md M047_TYPED_JSON_GROUP.md hat/hatSql/columnar_json_subcolumn_scan.go hat/hatSql/m047_typed_json_group_test.go scripts/benchmark-m047.sh scripts/deliver-m047.sh scripts/format-m047.sh scripts/race-m047.sh scripts/test-m047-package.sh scripts/test-m047.sh scripts/verify-m047-diff.sh scripts/verify-m047-docs.sh scripts/vet-m047.sh
git diff --cached --check
git commit -m "feat(sql): group typed JSON subcolumns without row decoding"
git push origin HEAD:master
