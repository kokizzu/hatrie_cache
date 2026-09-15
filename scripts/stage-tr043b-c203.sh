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

.PHONY: test-tr043b-c203
test-tr043b-c203:
	@bash ./scripts/test-tr043b-c203.sh

.PHONY: benchmark-tr043b-c203
benchmark-tr043b-c203:
	@bash ./scripts/benchmark-tr043b-c203.sh

.PHONY: format-tr043b-c203
format-tr043b-c203:
	@bash ./scripts/format-tr043b-c203.sh

.PHONY: verify-tr043b-c203
verify-tr043b-c203:
	@bash ./scripts/verify-tr043b-c203.sh

.PHONY: stage-tr043b-c203
stage-tr043b-c203:
	@bash ./scripts/stage-tr043b-c203.sh

.PHONY: inspect-staged-tr043b-c203
inspect-staged-tr043b-c203:
	@bash ./scripts/inspect-staged-tr043b-c203.sh

.PHONY: commit-tr043b-c203
commit-tr043b-c203:
	@bash ./scripts/commit-tr043b-c203.sh

.PHONY: push-tr043b-c203
push-tr043b-c203:
	@bash ./scripts/push-tr043b-c203.sh
EOF
mv "$makefile_staged" Makefile

git add BENCHMARK.md COMPACT_PEER_SESSION.md INSPIRATION_BACKLOG.md Makefile README.md TR043B_COMPACT_PEER_WRITE_BUFFER.md hat/hatPeer/compact_protocol.go hat/hatPeer/compact_protocol_benchmark_test.go hat/hatPeer/compact_protocol_test.go hat/hatPeer/compact_session.go scripts/benchmark-tr043b-c203.sh scripts/commit-tr043b-c203.sh scripts/format-tr043b-c203.sh scripts/inspect-staged-tr043b-c203.sh scripts/push-tr043b-c203.sh scripts/stage-tr043b-c203.sh scripts/test-tr043b-c203.sh scripts/verify-tr043b-c203.sh
