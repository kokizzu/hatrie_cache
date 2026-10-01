#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU39_SPACE_CHANGEFEED.md \
	hat/hatReplication/space_changefeed.go \
	hat/hatReplication/space_changefeed_test.go \
	hat/hatReplication/space_changefeed_benchmark_test.go \
	scripts/format-chg16-space-changefeed.sh \
	scripts/test-chg16-space-changefeed.sh \
	scripts/benchmark-chg16-space-changefeed.sh \
	scripts/test-chg16-space-changefeed-package.sh \
	scripts/race-chg16-space-changefeed.sh \
	scripts/vet-chg16-space-changefeed.sh \
	scripts/status-chg16-space-changefeed.sh \
	scripts/stage-chg16-space-changefeed.sh \
	scripts/commit-chg16-space-changefeed.sh \
	scripts/push-chg16-space-changefeed.sh
