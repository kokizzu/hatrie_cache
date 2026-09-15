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

.PHONY: test-parallel-read-fastpath-c208 benchmark-parallel-read-fastpath-c208 format-parallel-read-fastpath-c208 verify-parallel-read-fastpath-c208 inspect-parallel-read-fastpath-diff-c208 stage-parallel-read-fastpath-c208 commit-parallel-read-fastpath-c208 push-parallel-read-fastpath-c208
test-parallel-read-fastpath-c208:
	bash ./scripts/test-parallel-read-fastpath-c208.sh

benchmark-parallel-read-fastpath-c208:
	bash ./scripts/benchmark-parallel-read-fastpath-c208.sh

format-parallel-read-fastpath-c208:
	bash ./scripts/format-parallel-read-fastpath-c208.sh

verify-parallel-read-fastpath-c208:
	bash ./scripts/verify-parallel-read-fastpath-c208.sh

inspect-parallel-read-fastpath-diff-c208:
	bash ./scripts/inspect-parallel-read-fastpath-diff-c208.sh

stage-parallel-read-fastpath-c208:
	bash ./scripts/stage-parallel-read-fastpath-c208.sh

commit-parallel-read-fastpath-c208:
	bash ./scripts/commit-parallel-read-fastpath-c208.sh

push-parallel-read-fastpath-c208:
	bash ./scripts/push-parallel-read-fastpath-c208.sh
MAKEFILE

git add Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR055_SINGLE_REPLICA_READ_FASTPATH.md \
  hat/hatReplication/parallel_replica_read.go \
  hat/hatReplication/parallel_replica_read_fastpath_test.go \
  hat/hatReplication/parallel_replica_read_fastpath_benchmark_test.go \
  scripts/benchmark-parallel-read-fastpath-c208.sh \
  scripts/commit-parallel-read-fastpath-c208.sh \
  scripts/format-parallel-read-fastpath-c208.sh \
  scripts/inspect-parallel-read-fastpath-diff-c208.sh \
  scripts/push-parallel-read-fastpath-c208.sh \
  scripts/stage-parallel-read-fastpath-c208.sh \
  scripts/test-parallel-read-fastpath-c208.sh \
  scripts/verify-parallel-read-fastpath-c208.sh
