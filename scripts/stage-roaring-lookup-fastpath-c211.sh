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

.PHONY: test-roaring-lookup-fastpath-c211 benchmark-roaring-lookup-fastpath-c211 format-roaring-lookup-fastpath-c211 verify-roaring-lookup-fastpath-c211 inspect-roaring-lookup-fastpath-diff-c211 stage-roaring-lookup-fastpath-c211 commit-roaring-lookup-fastpath-c211 push-roaring-lookup-fastpath-c211
test-roaring-lookup-fastpath-c211:
	bash ./scripts/test-roaring-lookup-fastpath-c211.sh

benchmark-roaring-lookup-fastpath-c211:
	bash ./scripts/benchmark-roaring-lookup-fastpath-c211.sh

format-roaring-lookup-fastpath-c211:
	bash ./scripts/format-roaring-lookup-fastpath-c211.sh

verify-roaring-lookup-fastpath-c211:
	bash ./scripts/verify-roaring-lookup-fastpath-c211.sh

inspect-roaring-lookup-fastpath-diff-c211:
	bash ./scripts/inspect-roaring-lookup-fastpath-diff-c211.sh

stage-roaring-lookup-fastpath-c211:
	bash ./scripts/stage-roaring-lookup-fastpath-c211.sh

commit-roaring-lookup-fastpath-c211:
	bash ./scripts/commit-roaring-lookup-fastpath-c211.sh

push-roaring-lookup-fastpath-c211:
	bash ./scripts/push-roaring-lookup-fastpath-c211.sh
MAKEFILE

git add Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR058_ROARING_BITMAP_LOOKUP_FASTPATH.md \
  hat/hatDataStructure/roaring.go \
  hat/hatDataStructure/roaring_lookup_fastpath_benchmark_test.go \
  hat/hatDataStructure/roaring_lookup_fastpath_test.go \
  scripts/benchmark-roaring-lookup-fastpath-c211.sh \
  scripts/commit-roaring-lookup-fastpath-c211.sh \
  scripts/format-roaring-lookup-fastpath-c211.sh \
  scripts/inspect-roaring-lookup-fastpath-diff-c211.sh \
  scripts/push-roaring-lookup-fastpath-c211.sh \
  scripts/stage-roaring-lookup-fastpath-c211.sh \
  scripts/test-roaring-lookup-fastpath-c211.sh \
  scripts/verify-roaring-lookup-fastpath-c211.sh
