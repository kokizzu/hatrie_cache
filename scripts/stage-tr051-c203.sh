#!/usr/bin/env bash
set -euo pipefail

makefile_backup=$(mktemp)
makefile_staged=$(mktemp)
restore_makefile() {
	mv "$makefile_backup" Makefile
}
trap restore_makefile EXIT

cp Makefile "$makefile_backup"
git show HEAD:Makefile > "$makefile_staged"
cat >> "$makefile_staged" <<'EOF'

.PHONY: test-tr051-c203
test-tr051-c203:
	@bash ./scripts/test-tr051-c203.sh

.PHONY: benchmark-tr051-c203
benchmark-tr051-c203:
	@bash ./scripts/benchmark-tr051-c203.sh

.PHONY: format-tr051-c203
format-tr051-c203:
	@bash ./scripts/format-tr051-c203.sh

.PHONY: verify-tr051-c203
verify-tr051-c203:
	@bash ./scripts/verify-tr051-c203.sh

.PHONY: stage-tr051-c203
stage-tr051-c203:
	@bash ./scripts/stage-tr051-c203.sh

.PHONY: inspect-staged-tr051-c203
inspect-staged-tr051-c203:
	@bash ./scripts/inspect-staged-tr051-c203.sh

.PHONY: commit-tr051-c203
commit-tr051-c203:
	@bash ./scripts/commit-tr051-c203.sh

.PHONY: push-tr051-c203
push-tr051-c203:
	@bash ./scripts/push-tr051-c203.sh
EOF
mv "$makefile_staged" Makefile

git add BENCHMARK.md COMPACT_PEER_LISTENER.md INSPIRATION_BACKLOG.md Makefile README.md TR044_COMPACT_PEER_COMPRESSION.md TR051_COMPACT_PEER_CAPABILITY_NEGOTIATION.md hat/hatPeer/compact_listener.go hat/hatPeer/compact_listener_benchmark_test.go hat/hatPeer/compact_listener_test.go hat/hatPeer/compact_session.go scripts/benchmark-tr051-c203.sh scripts/commit-tr051-c203.sh scripts/format-tr051-c203.sh scripts/inspect-staged-tr051-c203.sh scripts/push-tr051-c203.sh scripts/stage-tr051-c203.sh scripts/test-tr051-c203.sh scripts/verify-tr051-c203.sh
