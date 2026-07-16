#!/usr/bin/env sh
set -eu

workflow=.github/workflows/ci.yml

fail() {
	echo "verify-ci: $*" >&2
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

require_job_timeout() {
	job=$1
	awk -v job="$job" '
		$0 ~ "^  " job ":" {
			in_job = 1
			next
		}
		in_job && $0 ~ "^  [[:alnum:]_-]+:" {
			exit
		}
		in_job && /timeout-minutes:/ {
			found = 1
		}
		END {
			exit(found ? 0 : 1)
		}
	' "$workflow" || fail "CI job $job is missing timeout-minutes"
}

require_file "$workflow"
require_file Makefile
require_executable scripts/check-config.sh
require_executable scripts/docker-build.sh
require_executable scripts/verify-ci.sh

require_pattern Makefile '^[[:space:]]*verify-ci:' "Makefile is missing verify-ci target"
require_pattern "$workflow" '^permissions:' "CI workflow is missing explicit permissions"
require_pattern "$workflow" 'contents:[[:space:]]*read' "CI workflow must keep contents: read"
require_pattern "$workflow" '^concurrency:' "CI workflow is missing concurrency cancellation"
require_pattern "$workflow" 'cancel-in-progress:[[:space:]]*true' "CI workflow must cancel superseded runs"
require_pattern "$workflow" 'run:[[:space:]]*make verify-ci' "CI workflow does not run make verify-ci"
require_pattern "$workflow" 'run:[[:space:]]*make verify-go' "CI workflow does not run make verify-go"
require_pattern "$workflow" 'run:[[:space:]]*make verify-c' "CI workflow does not run make verify-c"
require_pattern "$workflow" 'run:[[:space:]]*make verify-ops' "CI workflow does not run make verify-ops"
require_pattern "$workflow" 'run:[[:space:]]*make verify-frontend' "CI workflow does not run make verify-frontend"
require_pattern "$workflow" 'run:[[:space:]]*make docker-build' "CI workflow does not build the Docker image through make"
require_pattern "$workflow" 'cache-dependency-path:[[:space:]]*svelte-mpa/pnpm-lock.yaml' "frontend dependency cache is not tied to the pnpm lockfile"

for job in config go c-and-ops frontend docker; do
	require_pattern "$workflow" "^  $job:" "CI workflow is missing $job job"
	require_job_timeout "$job"
done

CONFIG_PATH=deploy/hatrie-cache.json ./scripts/check-config.sh >/dev/null
./scripts/check-config.sh \
	-monitoring-server \
	-monitoring-addr 127.0.0.1:0 \
	-monitoring-web-dir "" \
	-node-id node-a \
	-topology-path deploy/topology/sharded.json \
	-replication=true \
	-enforce-leader-writes=true \
	-grpc-addr 127.0.0.1:0 >/dev/null

if [ "${CI_VERIFY_DOCKER_COMPOSE:-0}" = "1" ]; then
	MONITORING_AUTH_TOKEN=ci docker compose -f deploy/docker-compose.production.yml config >/dev/null
fi

echo "verify-ci: ok"
