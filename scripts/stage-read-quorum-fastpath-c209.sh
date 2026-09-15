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

.PHONY: test-read-quorum-fastpath-c209 benchmark-read-quorum-fastpath-c209 format-read-quorum-fastpath-c209 verify-read-quorum-fastpath-c209 inspect-read-quorum-fastpath-diff-c209 stage-read-quorum-fastpath-c209 commit-read-quorum-fastpath-c209 push-read-quorum-fastpath-c209
test-read-quorum-fastpath-c209:
	bash ./scripts/test-read-quorum-fastpath-c209.sh

benchmark-read-quorum-fastpath-c209:
	bash ./scripts/benchmark-read-quorum-fastpath-c209.sh

format-read-quorum-fastpath-c209:
	bash ./scripts/format-read-quorum-fastpath-c209.sh

verify-read-quorum-fastpath-c209:
	bash ./scripts/verify-read-quorum-fastpath-c209.sh

inspect-read-quorum-fastpath-diff-c209:
	bash ./scripts/inspect-read-quorum-fastpath-diff-c209.sh

stage-read-quorum-fastpath-c209:
	bash ./scripts/stage-read-quorum-fastpath-c209.sh

commit-read-quorum-fastpath-c209:
	bash ./scripts/commit-read-quorum-fastpath-c209.sh

push-read-quorum-fastpath-c209:
	bash ./scripts/push-read-quorum-fastpath-c209.sh
MAKEFILE

git add Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR056_SINGLE_NODE_READ_QUORUM_FASTPATH.md \
  hat/hatReplication/read_quorum.go \
  hat/hatReplication/read_quorum_fastpath_benchmark_test.go \
  hat/hatReplication/read_quorum_fastpath_test.go \
  scripts/benchmark-read-quorum-fastpath-c209.sh \
  scripts/commit-read-quorum-fastpath-c209.sh \
  scripts/format-read-quorum-fastpath-c209.sh \
  scripts/inspect-read-quorum-fastpath-diff-c209.sh \
  scripts/push-read-quorum-fastpath-c209.sh \
  scripts/stage-read-quorum-fastpath-c209.sh \
  scripts/test-read-quorum-fastpath-c209.sh \
  scripts/verify-read-quorum-fastpath-c209.sh
