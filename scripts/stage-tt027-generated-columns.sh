#!/usr/bin/env bash
set -euo pipefail

git add \
	ENGINE_IDEAS.md \
	Makefile \
	TT027_GENERATED_COLUMNS.md \
	hat/hatSchema/materialized.go \
	hat/hatSchema/tt027_generated_columns.go \
	hat/hatSchema/tt027_generated_columns_benchmark_test.go \
	hat/hatSchema/tt027_generated_columns_test.go \
	scripts/benchmark-tt027-generated-columns.sh \
	scripts/commit-tt027-generated-columns.sh \
	scripts/format-tt027-generated-columns.sh \
	scripts/push-tt027-generated-columns.sh \
	scripts/stage-tt027-generated-columns.sh \
	scripts/test-tt027-generated-columns.sh \
	scripts/verify-tt027-generated-columns.sh
