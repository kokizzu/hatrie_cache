#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	TT006_HOT_STANDBY_WAL_CATCHUP.md \
	hat/hatReplication/tt006_hot_standby.go \
	hat/hatReplication/tt006_hot_standby_benchmark_test.go \
	hat/hatReplication/tt006_hot_standby_test.go \
	scripts/tt006-hot-standby.sh \
	scripts/stage-tt006-hot-standby.sh \
	scripts/review-tt006-hot-standby.sh \
	scripts/commit-tt006-hot-standby.sh \
	scripts/push-tt006-hot-standby.sh
