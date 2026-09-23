#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	T236_MAILBOX.md \
	hat/hatFiber/mailbox.go \
	hat/hatFiber/t236_mailbox_baseline_benchmark_test.go \
	hat/hatFiber/t236_mailbox_benchmark_test.go \
	hat/hatFiber/t236_mailbox_test.go \
	hat/hatFiber/t236_mailbox_waiters_test.go \
	scripts/benchmark-t236-before.sh \
	scripts/benchmark-t236-mailbox.sh \
	scripts/benchmark-t236.sh \
	scripts/commit-t236.sh \
	scripts/format-t236.sh \
	scripts/push-t236.sh \
	scripts/race-t236.sh \
	scripts/stage-t236.sh \
	scripts/test-t236-package.sh \
	scripts/test-t236.sh \
	scripts/vet-t236.sh
