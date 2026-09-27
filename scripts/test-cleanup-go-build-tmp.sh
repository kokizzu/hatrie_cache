#!/usr/bin/env bash
set -euo pipefail

test_root="$(mktemp -d /tmp/hatrie-go-build-test-root.XXXXXX)"
plan="$test_root/cleanup.plan"
fixture="$test_root/go-build-fixture"
cleanup() {
	rm -rf "$test_root"
}
trap cleanup EXIT

mkdir -p "$fixture"
printf 'fixture\n' > "$fixture/output"

HATRIE_GO_BUILD_TMP_ROOT="$test_root" \
	HATRIE_GO_BUILD_CLEANUP_PLAN="$plan" \
	HATRIE_GO_BUILD_MIN_AGE_SECONDS=0 \
	bash scripts/cleanup-go-build-tmp.sh preview
[[ -s "$plan" ]]

HATRIE_GO_BUILD_TMP_ROOT="$test_root" \
	HATRIE_GO_BUILD_CLEANUP_PLAN="$plan" \
	HATRIE_GO_BUILD_MIN_AGE_SECONDS=0 \
	bash scripts/cleanup-go-build-tmp.sh apply
[[ ! -e "$fixture" ]]
[[ ! -e "$plan" ]]

HATRIE_GO_BUILD_TMP_ROOT="$test_root" \
	HATRIE_GO_BUILD_CLEANUP_PLAN="$plan" \
	HATRIE_GO_BUILD_MIN_AGE_SECONDS=0 \
	bash scripts/cleanup-go-build-tmp.sh preview
[[ ! -e "$plan" ]]
