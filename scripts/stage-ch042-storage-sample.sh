#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	CH042_STORAGE_AWARE_SAMPLING.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	hat/hatSql/ch042_storage_sample_benchmark_test.go \
	hat/hatSql/ch042_storage_sample_test.go \
	hat/hatSql/contracts.go \
	hat/hatSql/query.go \
	scripts/benchmark-ch042-storage-sample-before.sh \
	scripts/benchmark-ch042-storage-sample.sh \
	scripts/commit-ch042-storage-sample.sh \
	scripts/format-ch042-storage-sample.sh \
	scripts/push-ch042-storage-sample.sh \
	scripts/race-ch042-storage-sample.sh \
	scripts/stage-ch042-storage-sample.sh \
	scripts/test-ch042-sampling-compat.sh \
	scripts/test-ch042-sql-package.sh \
	scripts/test-ch042-storage-sample.sh \
	scripts/vet-ch042-storage-sample.sh
git diff --cached --check
git diff --cached --stat
