#!/bin/sh
set -eu

git diff --check
git add \
	Makefile \
	BENCHMARK.md \
	CONFIG_WATCH.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU27_PEER_CONFIG_WATCH.md \
	hat/hatTopology/config_watch.go \
	hat/hatTopology/config_watch_peer.go \
	hat/hatTopology/config_watch_peer_benchmark_test.go \
	hat/hatTopology/config_watch_peer_test.go \
	hat/hatTopology/config_watch_peer_transport_test.go \
	scripts/format-config-watch-peer.sh \
	scripts/test-config-watch-peer.sh
git diff --cached --check
git commit -m 'feat(topology): add peer configuration watch [skip ci]'
git push origin codex/next-inspiration-round56-chain
