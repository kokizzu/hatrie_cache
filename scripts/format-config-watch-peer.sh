#!/bin/sh
set -eu

gofmt -w \
	hat/hatTopology/config_watch.go \
	hat/hatTopology/config_watch_peer.go \
	hat/hatTopology/config_watch_peer_test.go \
	hat/hatTopology/config_watch_peer_transport_test.go \
	hat/hatTopology/config_watch_peer_benchmark_test.go
