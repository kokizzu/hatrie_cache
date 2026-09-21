#!/usr/bin/env bash
set -euo pipefail

test_path="$(mktemp -d /tmp/go-build-cleanup-test.XXXXXX)"
cleanup() {
	if [[ -e "$test_path" ]]; then
		rm -rf -- "$test_path"
	fi
}
trap cleanup EXIT

bash ./scripts/cleanup-go-build-tmp.sh plan
bash ./scripts/cleanup-go-build-tmp.sh apply
if [[ -e "$test_path" ]]; then
	printf 'cleanup test directory still exists: %s\n' "$test_path" >&2
	exit 1
fi
