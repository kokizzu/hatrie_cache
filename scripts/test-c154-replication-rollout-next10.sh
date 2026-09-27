#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154-test.XXXXXX")
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatCache -run '^TestC154ReplicationSchemaRolloutSwitchesContractsAtCompletion$' -count=1
