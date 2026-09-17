#!/bin/sh
set -eu

mode=${1:-}
case "$mode" in
stage)
	git add -- BENCHMARK.md INSPIRATION_BACKLOG.md README.md STORAGE_TIERS.md STORAGE_TIER_MOVEMENT.md Makefile hat/hatStorage/ch015_storage_tier_move.go hat/hatStorage/ch015_storage_tier_move_benchmark_test.go hat/hatStorage/ch015_storage_tier_move_test.go scripts/benchmark-ch015-storage-tier-movement-before.sh scripts/benchmark-ch015-storage-tier-movement.sh scripts/deliver-ch015-storage-tier-movement.sh scripts/format-ch015-storage-tier-movement.sh scripts/race-ch015-storage-tier-movement.sh scripts/review-ch015-storage-tier-movement.sh scripts/test-ch015-storage-tier-movement-package.sh scripts/test-ch015-storage-tier-movement.sh scripts/vet-ch015-storage-tier-movement.sh
	;;
commit)
	git commit -m 'feat: add explicit storage tier movement planning [skip ci]'
	;;
push)
	git push origin HEAD:master
	;;
verify)
	git diff --cached --check
	git status --short
	git log -1 --oneline
	;;
*)
	echo "usage: $0 {stage|commit|push|verify}" >&2
	exit 2
	;;
esac
