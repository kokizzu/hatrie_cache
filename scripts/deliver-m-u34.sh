#!/usr/bin/env bash
set -euo pipefail

mode="${1:?usage: deliver-m-u34.sh stage|commit|push}"
case "$mode" in
stage)
	git add \
		BENCHMARK.md \
		Makefile \
		MU034_HISTORICAL_SUBSCRIPTION_CHECKPOINTS.md \
		PRODUCT_IDEA_GAPS.md \
		README.md \
		hat/hatCache/journal_checkpoint.go \
		hat/hatCache/journal_sink.go \
		hat/hatCache/journal_subscription.go \
		hat/hatCache/m_u34_subscription_checkpoint_benchmark_test.go \
		hat/hatCache/m_u34_subscription_checkpoint_test.go \
		scripts/benchmark-m-u34-baseline.sh \
		scripts/benchmark-m-u34.sh \
		scripts/deliver-m-u34.sh \
		scripts/format-m-u34.sh \
		scripts/race-m-u34.sh \
		scripts/review-m-u34.sh \
		scripts/test-journal-subscription.sh \
		scripts/test-m-u34-package.sh \
		scripts/test-m-u34.sh \
		scripts/vet-m-u34.sh
	git diff --cached --check
	git diff --cached --name-status
	;;
commit)
	git commit -m "feat: add checkpointed journal subscriptions [skip ci]"
	;;
push)
	git push -u origin codex/next-inspiration-round51
	;;
*)
	printf 'unknown delivery mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
