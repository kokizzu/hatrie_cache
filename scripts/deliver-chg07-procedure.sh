#!/usr/bin/env bash
set -euo pipefail

files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	Makefile
	PRODUCT_IDEA_GAPS.md
	README.md
	TU03_STORED_PROCEDURE_REGISTRY.md
	hat/hatSql/procedure_registry.go
	hat/hatSql/t_u03_procedure_registry_benchmark_test.go
	hat/hatSql/t_u03_procedure_registry_test.go
	scripts/benchmark-chg07-procedure.sh
	scripts/check-chg07-diff.sh
	scripts/deliver-chg07-procedure.sh
	scripts/format-chg07-procedure.sh
	scripts/race-chg07-package.sh
	scripts/test-chg07-package.sh
	scripts/test-chg07-procedure.sh
	scripts/vet-chg07-package.sh
)

git add "${files[@]}"
git diff --cached --check
git diff --cached --stat
git commit -m 'feat(sql): add read-only procedure registry [skip ci]'
git push origin HEAD
