#!/usr/bin/env bash
set -euo pipefail

makefile_backup="$(mktemp)"
restore_makefile() {

  cp "$makefile_backup" Makefile
  rm -f "$makefile_backup"
}
trap restore_makefile EXIT

cp Makefile "$makefile_backup"
git show HEAD:Makefile > Makefile
cat >> Makefile <<'MAKEFILE'

.PHONY: test-upsert-batch-small-vector-c210 benchmark-upsert-batch-small-vector-c210 benchmark-upsert-batch-baseline-c210 benchmark-upsert-batch-control-c210 format-upsert-batch-small-vector-c210 verify-upsert-batch-small-vector-c210 inspect-upsert-batch-small-vector-diff-c210 stage-upsert-batch-small-vector-c210 commit-upsert-batch-small-vector-c210 push-upsert-batch-small-vector-c210
test-upsert-batch-small-vector-c210:
	bash ./scripts/test-upsert-batch-small-vector-c210.sh

benchmark-upsert-batch-small-vector-c210:
	bash ./scripts/benchmark-upsert-batch-small-vector-c210.sh

benchmark-upsert-batch-baseline-c210:
	bash ./scripts/benchmark-upsert-batch-baseline-c210.sh

benchmark-upsert-batch-control-c210:
	bash ./scripts/benchmark-upsert-batch-control-c210.sh

format-upsert-batch-small-vector-c210:
	bash ./scripts/format-upsert-batch-small-vector-c210.sh

verify-upsert-batch-small-vector-c210:
	bash ./scripts/verify-upsert-batch-small-vector-c210.sh

inspect-upsert-batch-small-vector-diff-c210:
	bash ./scripts/inspect-upsert-batch-small-vector-diff-c210.sh

stage-upsert-batch-small-vector-c210:
	bash ./scripts/stage-upsert-batch-small-vector-c210.sh

commit-upsert-batch-small-vector-c210:
	bash ./scripts/commit-upsert-batch-small-vector-c210.sh

push-upsert-batch-small-vector-c210:
	bash ./scripts/push-upsert-batch-small-vector-c210.sh
MAKEFILE

git add Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR057_UPSERT_BATCH_SMALL_VECTOR.md \
  hat/hatDataStructure/upsert_batch.go \
  hat/hatDataStructure/upsert_batch_small_vector_benchmark_test.go \
  hat/hatDataStructure/upsert_batch_small_vector_test.go \
  scripts/benchmark-upsert-batch-baseline-c210.sh \
  scripts/benchmark-upsert-batch-control-c210.sh \
  scripts/benchmark-upsert-batch-small-vector-c210.sh \
  scripts/commit-upsert-batch-small-vector-c210.sh \
  scripts/format-upsert-batch-small-vector-c210.sh \
  scripts/inspect-upsert-batch-small-vector-diff-c210.sh \
  scripts/push-upsert-batch-small-vector-c210.sh \
  scripts/stage-upsert-batch-small-vector-c210.sh \
  scripts/test-upsert-batch-small-vector-c210.sh \
  scripts/verify-upsert-batch-small-vector-c210.sh
