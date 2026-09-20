#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU20_ONLINE_SPACE_UPGRADE.md \
	hat/hatDataStructure/online_space_upgrade.go \
	hat/hatDataStructure/tu20_online_space_upgrade_baseline_benchmark_test.go \
	hat/hatDataStructure/tu20_online_space_upgrade_benchmark_test.go \
	hat/hatDataStructure/tu20_online_space_upgrade_test.go \
	scripts/benchmark-tu20-before.sh \
	scripts/benchmark-tu20.sh \
	scripts/commit-tu20.sh \
	scripts/format-tu20.sh \
	scripts/push-tu20.sh \
	scripts/test-tu20.sh \
	scripts/verify-tu20.sh

git commit -m "feat(schema): add online space upgrades"
