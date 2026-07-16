#!/usr/bin/env sh
set -eu

peer=${CLUSTER_PEER:-http://127.0.0.1:8080}
probe_nodes=${CLUSTER_PROBE_NODES:-true}

set -- cluster status -peer "$peer"
case "$probe_nodes" in
	0|false|FALSE|no|NO|off|OFF)
		set -- "$@" -probe-nodes=false
		;;
esac

exec go run ./cmd/hatrie-cli "$@"
