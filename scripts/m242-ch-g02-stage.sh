#!/usr/bin/env bash
set -euo pipefail
git add -- \
	BENCHMARK.md \
	CH002_GRACE_HASH_JOIN.md \
	IDEA_GAP_CATALOG.md \
	Makefile \
	README.md \
	hat/hatSql/ch002_grace_hash_benchmark_test.go \
	scripts/m242-ch-g02-benchmark.sh \
	scripts/m242-ch-g02-commit.sh \
	scripts/m242-ch-g02-docs.sh \
	scripts/m242-ch-g02-format.sh \
	scripts/m242-ch-g02-push.sh \
	scripts/m242-ch-g02-stage.sh \
	scripts/m242-ch-g02-test.sh
