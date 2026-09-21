#!/usr/bin/env bash
set -euo pipefail

for file in \
	ENGINE_IDEAS.md \
	README.md \
	BENCHMARK.md \
	TT025_ONLINE_UNIQUENESS.md \
	hat/hatSchema/materialized.go \
	hat/hatSchema/tt025_online_uniqueness_test.go \
	hat/hatSchema/tt025_online_uniqueness_benchmark_test.go \
	scripts/test-tt025-online-uniqueness.sh \
	scripts/benchmark-tt025-online-uniqueness.sh \
	scripts/format-tt025-online-uniqueness.sh \
	scripts/race-tt025-online-uniqueness.sh \
	scripts/test-tt025-package.sh \
	scripts/verify-tt025-online-uniqueness.sh \
	scripts/stage-tt025-online-uniqueness.sh \
	scripts/commit-tt025-online-uniqueness.sh \
	scripts/push-tt025-online-uniqueness.sh; do
	test -f "$file"
done

rg -q 'BuildUniqueIndex' hat/hatSchema/materialized.go hat/hatSchema/tt025_online_uniqueness_test.go
rg -q 'ErrMaterializedSourceUniqueIndexViolation' hat/hatSchema/materialized.go hat/hatSchema/tt025_online_uniqueness_test.go
rg -q 'TT-025 Online Uniqueness Validation' BENCHMARK.md TT025_ONLINE_UNIQUENESS.md
rg -q 'TT-025.*BuildUniqueIndex' ENGINE_IDEAS.md
rg -q 'TT025_ONLINE_UNIQUENESS.md' README.md

bash -n \
	scripts/test-tt025-online-uniqueness.sh \
	scripts/benchmark-tt025-online-uniqueness.sh \
	scripts/format-tt025-online-uniqueness.sh \
	scripts/race-tt025-online-uniqueness.sh \
	scripts/test-tt025-package.sh \
	scripts/verify-tt025-online-uniqueness.sh \
	scripts/stage-tt025-online-uniqueness.sh \
	scripts/commit-tt025-online-uniqueness.sh \
	scripts/push-tt025-online-uniqueness.sh

git diff --check
git diff --cached --check
printf '%s\n' 'TT-025 uniqueness verification passed'
