#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md M036_ARRANGEMENT_HYDRATION_ADMISSION.md \
	hat/hatSql/m_u36_arrangement_hydration_admission.go \
	hat/hatSql/m_u36_arrangement_hydration_admission_test.go \
	hat/hatSql/m_u36_arrangement_hydration_admission_benchmark_test.go \
	hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_join_arrangements.go \
	scripts/format-mu036.sh scripts/test-mu036.sh scripts/benchmark-mu036.sh \
	scripts/race-mu036.sh scripts/vet-mu036.sh scripts/test-mu036-package.sh \
	scripts/commit-mu036.sh scripts/push-mu036.sh
git diff --cached --check
git commit -m "hatSql: add arrangement hydration admission"
