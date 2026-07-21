#!/usr/bin/env sh
set -eu

fail() {
	echo "verify-local: $*" >&2
	exit 1
}

require_file() {
	path=$1
	[ -f "$path" ] || fail "missing $path"
}

require_executable() {
	path=$1
	[ -x "$path" ] || fail "$path is not executable"
}

require_pattern() {
	path=$1
	pattern=$2
	message=$3
	grep -Eq "$pattern" "$path" || fail "$message"
}

[ ! -e .github/workflows/ci.yml ] || fail "GitHub Actions workflow must remain removed"
require_file Makefile
require_file deploy/hatrie-cache.json
require_executable scripts/check-config.sh
require_executable scripts/docker-build.sh
require_executable scripts/benchmark-smoke.sh
require_executable scripts/update-benchmark-md.sh
require_executable scripts/verify-benchmark-md-update.sh
require_executable scripts/verify-local.sh

require_pattern Makefile '^[[:space:]]*verify:[[:space:]]+verify-local' "Makefile verify target must use verify-local"
require_pattern Makefile '^[[:space:]]*verify-local:' "Makefile is missing verify-local target"
require_pattern Makefile '^[[:space:]]*verify-local-contract:' "Makefile is missing verify-local-contract target"
require_pattern Makefile '^[[:space:]]*bench-smoke:' "Makefile is missing bench-smoke target"
require_pattern Makefile '^[[:space:]]*benchmark-md:' "Makefile is missing benchmark-md target"
require_pattern Makefile '^[[:space:]]*verify-benchmark-md-update:' "Makefile is missing benchmark md update verifier target"

CONFIG_PATH=deploy/hatrie-cache.json ./scripts/check-config.sh >/dev/null
./scripts/check-config.sh \
	-monitoring-server \
	-monitoring-addr 127.0.0.1:0 \
	-monitoring-web-dir "" \
	-node-id node-a \
	-topology-path deploy/topology/sharded.json \
	-replication=true \
	-replication-mode command \
	-enforce-leader-writes=true \
	-grpc-addr 127.0.0.1:0 >/dev/null

if [ "${VERIFY_LOCAL_DOCKER_COMPOSE:-0}" = "1" ]; then
	MONITORING_AUTH_TOKEN=local docker compose -f deploy/docker-compose.production.yml config >/dev/null
fi

echo "verify-local: ok"
