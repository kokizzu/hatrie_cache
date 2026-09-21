#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	CH020_ZERO_COPY_PART_SHARING.md \
	hat/hatMerkle/ch020_zero_copy_part_sharing.go \
	hat/hatMerkle/ch020_zero_copy_part_sharing_benchmark_test.go \
	hat/hatMerkle/ch020_zero_copy_part_sharing_test.go \
	scripts/ch020-zero-copy-part-sharing.sh \
	scripts/stage-ch020-zero-copy-part-sharing.sh \
	scripts/review-ch020-zero-copy-part-sharing.sh \
	scripts/commit-ch020-zero-copy-part-sharing.sh \
	scripts/push-ch020-zero-copy-part-sharing.sh
