#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatTopology/config_watch.go \
	hat/hatTopology/config_watch_peer.go \
	hat/hatTopology/tu27_peer_watch_test.go
