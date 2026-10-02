#!/usr/bin/env bash
set -euo pipefail

git switch -c codex/tu27-config-watch
git add \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	CONFIG_WATCH_PEER.md \
	hat/hatTopology/config_watch.go \
	hat/hatTopology/config_watch_peer.go \
	hat/hatTopology/config_watch_peer_benchmark_test.go \
	hat/hatTopology/tu27_peer_watch_test.go \
	scripts/benchmark-tu27.sh \
	scripts/cleanup-tu27-worktree.sh \
	scripts/deliver-tu27.sh \
	scripts/format-tu27.sh \
	scripts/race-tu27.sh \
	scripts/status-tu27.sh \
	scripts/test-tu27-package.sh \
	scripts/test-tu27-red.sh \
	scripts/test-tu27.sh \
	scripts/verify-tu27.sh \
	scripts/vet-tu27.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add peer config watch reconnects [skip ci]'
git push -u origin codex/tu27-config-watch
